package expv2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
)

// func TestPerSeriesBucketsAddSample(t *testing.T) {
// t.Parallel()

//aggregationPeriod := int64(3 * time.Second)
//r := metrics.NewRegistry()
//ts := metrics.TimeSeries{
//Metric: r.MustNewMetric("metric1", metrics.Counter),
//Tags:   r.RootTagSet(),
//}

//// Case: Add when empty
//buckets := newPerSeriesBuckets()
//buckets.AddSample(53, metrics.Sample{
//TimeSeries: ts,
//Value:      1.9,
//}, aggregationPeriod)
//require.Len(t, buckets, 1)

// b, ok := buckets[ts]
// require.True(t, ok)

// assert.Equal(t, time.Unix(53*3+1, int64(500*time.Millisecond)), b.Time)
// assert.Equal(t, ts, b.TimeSeries)

// counter, ok := b.Sink.(*metrics.CounterSink)
// require.True(t, ok)
// assert.Equal(t, 1.9, counter.Value)

//// Case: Add to the same bucket so Increment
//buckets.AddSample(53, metrics.Sample{
//TimeSeries: ts,
//Value:      49,
//}, aggregationPeriod)
//require.Len(t, buckets, 1)
//assert.Equal(t, time.Unix(53*3+1, int64(500*time.Millisecond)), b.Time)
//assert.Equal(t, ts, b.TimeSeries)
//counter, ok = b.Sink.(*metrics.CounterSink)
//require.True(t, ok)
//assert.Equal(t, 50.9, counter.Value)

//// Case: Add another key
//buckets.AddSample(53, metrics.Sample{
//TimeSeries: metrics.TimeSeries{
//Metric: ts.Metric,
//Tags:   r.RootTagSet().With("tag1", "value1"),
//},
//Value: 49,
//}, aggregationPeriod)
//require.Len(t, buckets, 2)
//}

// func TestCollectorBucketing(t *testing.T) {
// t.Parallel()

// r := metrics.NewRegistry()
// m1, err := r.NewMetric("metric1", metrics.Counter)
// require.NoError(t, err)

// tags := r.RootTagSet().With("t1", "v1")
// samples := metrics.Samples(make([]metrics.Sample, 3))

//for i := 0; i < len(samples); i++ {
//samples[i].TimeSeries = metrics.TimeSeries{
//Metric: m1,
//Tags:   tags,
//}
//samples[i].Value = 1.0
//samples[i].Time = time.Unix(int64(i*10), 0)
//}
//c := collector{
//aggregationPeriod: 3 * time.Second,
//timeBuckets:       make(map[int64]perSeriesBuckets),
//}
//c.bucketing([]metrics.SampleContainer{samples})
//assert.Len(t, c.timeBuckets, 3)
//}

func TestCollectorExpiredBucketsNoExipired(t *testing.T) {
	t.Parallel()

	c := collector{
		aggregationPeriod: 3 * time.Second,
		waitPeriod:        1 * time.Second,
		nowfn: func() time.Time {
			return time.Unix(10, 0)
		},
		timeBuckets: map[int64]map[metrics.TimeSeries]aggregatedMetric{
			6: {},
		},
	}
	require.Nil(t, c.expiredBuckets())
}

func TestCollectorExpiredBuckets(t *testing.T) {
	t.Parallel()

	r := metrics.NewRegistry()
	m1, err := r.NewMetric("metric1", metrics.Counter)
	require.NoError(t, err)

	ts1 := metrics.TimeSeries{
		Metric: m1,
		Tags:   r.RootTagSet().With("t1", "v1"),
	}
	ts2 := metrics.TimeSeries{
		Metric: m1,
		Tags:   r.RootTagSet().With("t1", "v2"),
	}

	c := collector{
		aggregationPeriod: 3 * time.Second,
		waitPeriod:        1 * time.Second,
		nowfn: func() time.Time {
			return time.Unix(10, 0)
		},
		timeBuckets: map[int64]map[metrics.TimeSeries]aggregatedMetric{
			3: {
				ts1: &counter{Sum: 10},
				ts2: &counter{Sum: 4},
			},
		},
	}
	expired := c.expiredBuckets()
	require.Len(t, expired, 1)

	assert.NotZero(t, expired[0].Time)

	exp := map[metrics.TimeSeries]aggregatedMetric{
		ts1: &counter{Sum: 10},
		ts2: &counter{Sum: 4},
	}
	assert.Equal(t, exp, expired[0].Sinks)
}

func TestCollectorExpiredBucketsCutoff(t *testing.T) {
	t.Parallel()

	c := collector{
		aggregationPeriod: 3 * time.Second,
		waitPeriod:        1 * time.Second,
		nowfn: func() time.Time {
			return time.Unix(10, 0)
		},
		timeBuckets: map[int64]map[metrics.TimeSeries]aggregatedMetric{
			3: {},
			6: {},
			9: {},
		},
	}
	expired := c.expiredBuckets()
	require.Len(t, expired, 1)
	assert.Len(t, c.timeBuckets, 2)
	assert.NotContains(t, c.timeBuckets, 3)

	require.Len(t, expired, 1)
	expDateTime := time.Unix(10, int64(500*time.Millisecond)).UTC()
	assert.Equal(t, expDateTime, expired[0].Time)
}

func TestCollectorBucketID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		unixSeconds int64
		unixNano    int64
		exp         int64
	}{
		{0, 0, 0},
		{2, 0, 0},
		{3, 0, 1},
		{28, 0, 9},
		{59, 7, 19},
	}

	c := collector{aggregationPeriod: 3 * time.Second}
	for _, tc := range tests {
		assert.Equal(t, tc.exp, c.bucketID(time.Unix(tc.unixSeconds, 0)))
	}
}

func TestCollectorTimeFromBucketID(t *testing.T) {
	t.Parallel()

	c := collector{aggregationPeriod: 3 * time.Second}

	// exp = Time(bucketID * 3s + (3s/2)) = Time(49 * 3s + (1.5s))
	exp := time.Date(1970, time.January, 1, 0, 2, 28, int(500*time.Millisecond), time.UTC)
	assert.Equal(t, exp, c.timeFromBucketID(49))
}

func TestCollectorBucketCutoffID(t *testing.T) {
	t.Parallel()

	c := collector{
		aggregationPeriod: 3 * time.Second,
		waitPeriod:        1 * time.Second,
		nowfn: func() time.Time {
			// 1st May 2023 - 01:06:06 + 8ns
			return time.Date(2023, time.May, 1, 1, 6, 6, 8, time.UTC)
		},
	}
	// exp = floor((now-1s)/3s) = floor(1682903165/3)
	assert.Equal(t, int64(560967721), c.bucketCutoffID())
}

func TestBucketQPush(t *testing.T) {
	t.Parallel()

	bq := bucketQ{}
	bq.Push([]timeBucket{{Time: time.Unix(1, 0)}})
	require.Len(t, bq.buckets, 1)
}

func TestBucketQPopAll(t *testing.T) {
	t.Parallel()
	bq := bucketQ{
		buckets: []timeBucket{
			{Time: time.Unix(1, 0)},
			{Time: time.Unix(2, 0)},
		},
	}
	buckets := bq.PopAll()
	require.Len(t, buckets, 2)
	assert.NotZero(t, buckets[0].Time)

	assert.NotNil(t, bq.buckets)
	assert.Empty(t, bq.buckets)
}

func TestBucketQPushPopConcurrency(t *testing.T) {
	t.Parallel()
	var (
		count = 0
		bq    = bucketQ{}
		sink  = &counter{}

		stop = time.After(100 * time.Millisecond)
		pop  = make(chan struct{}, 10)
		done = make(chan struct{})
	)

	go func() {
		for {
			select {
			case <-done:
				close(pop)
				return
			case <-pop:
				b := bq.PopAll()
				_ = append(b, timeBucket{})
			}
		}
	}()

	now := time.Now()
	for {
		select {
		case <-stop:
			close(done)
			return
		default:
			count++
			bq.Push([]timeBucket{
				{
					Time: now,
					Sinks: map[metrics.TimeSeries]aggregatedMetric{
						{}: sink,
					},
				},
			})

			if count%5 == 0 { // a fixed-arbitrary flush rate
				pop <- struct{}{}
			}
		}
	}
}

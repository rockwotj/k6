package expv2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/metrics"
)

func TestPerSeriesBucketsAddSample(t *testing.T) {
	t.Parallel()

	aggregationPeriod := int64(3 * time.Second)
	r := metrics.NewRegistry()
	ts := metrics.TimeSeries{
		Metric: r.MustNewMetric("metric1", metrics.Counter),
		Tags:   r.RootTagSet(),
	}

	// Case: Add when empty
	buckets := newPerSeriesBuckets()
	buckets.AddSample(53, metrics.Sample{
		TimeSeries: ts,
		Value:      1.9,
	}, aggregationPeriod)
	require.Len(t, buckets, 1)

	b, ok := buckets[ts]
	require.True(t, ok)

	assert.Equal(t, time.Unix(53*3+1, int64(500*time.Millisecond)), b.Time)
	assert.Equal(t, ts, b.TimeSeries)

	counter, ok := b.Sink.(*metrics.CounterSink)
	require.True(t, ok)
	assert.Equal(t, 1.9, counter.Value)

	// Case: Add to the same bucket so Increment
	buckets.AddSample(53, metrics.Sample{
		TimeSeries: ts,
		Value:      49,
	}, aggregationPeriod)
	require.Len(t, buckets, 1)
	assert.Equal(t, time.Unix(53*3+1, int64(500*time.Millisecond)), b.Time)
	assert.Equal(t, ts, b.TimeSeries)
	counter, ok = b.Sink.(*metrics.CounterSink)
	require.True(t, ok)
	assert.Equal(t, 50.9, counter.Value)

	// Case: Add another key
	buckets.AddSample(53, metrics.Sample{
		TimeSeries: metrics.TimeSeries{
			Metric: ts.Metric,
			Tags:   r.RootTagSet().With("tag1", "value1"),
		},
		Value: 49,
	}, aggregationPeriod)
	require.Len(t, buckets, 2)
}

func TestCollectorBucketing(t *testing.T) {
	t.Parallel()

	r := metrics.NewRegistry()
	m1, err := r.NewMetric("metric1", metrics.Counter)
	require.NoError(t, err)

	tags := r.RootTagSet().With("t1", "v1")
	samples := metrics.Samples(make([]metrics.Sample, 3))

	for i := 0; i < len(samples); i++ {
		samples[i].TimeSeries = metrics.TimeSeries{
			Metric: m1,
			Tags:   tags,
		}
		samples[i].Value = 1.0
		samples[i].Time = time.Unix(int64(i*10), 0)
	}
	c := collector{
		aggregationPeriod: 3 * time.Second,
		timeBuckets:       make(map[int64]perSeriesBuckets),
	}
	c.bucketing([]metrics.SampleContainer{samples})
	assert.Len(t, c.timeBuckets, 3)
}

func TestCollectorExpiredBuckets(t *testing.T) {
	t.Parallel()

	r := metrics.NewRegistry()
	m1, err := r.NewMetric("metric1", metrics.Counter)
	require.NoError(t, err)

	ts := metrics.TimeSeries{
		Metric: m1,
		Tags:   r.RootTagSet().With("t1", "v1"),
	}

	c := collector{
		aggregationPeriod: 3 * time.Second,
		waitPeriod:        1 * time.Second,
		nowfn: func() time.Time {
			return time.Unix(10, 0)
		},
		timeBuckets: map[int64]perSeriesBuckets{
			3: map[metrics.TimeSeries]timeBucket{
				ts: {Time: time.Unix(1, 0)},
			},
			6: map[metrics.TimeSeries]timeBucket{
				ts: {Time: time.Unix(2, 0)},
			},
			9: map[metrics.TimeSeries]timeBucket{
				ts: {Time: time.Unix(3, 0)},
			},
		},
	}
	expired := c.expiredBuckets()
	require.Len(t, expired, 1)
	assert.Len(t, c.timeBuckets, 2)
	assert.NotContains(t, c.timeBuckets, 3)

	assert.Equal(t, time.Unix(1, 0), expired[0].Time)
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

func TestBucketQPushPopConcurrency(t *testing.T) {
	t.Parallel()
	var (
		counter = 0
		bq      = bucketQ{}
		sink    = metrics.NewSink(metrics.Counter)

		stop = time.After(1 * time.Second)
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
				b := bq.Buckets()
				_ = append(b, timeBucket{})
			}
		}
	}()

	for {
		select {
		case <-stop:
			close(done)
			return
		default:
			counter++
			bq.Push([]timeBucket{
				{
					Sink:       sink,
					TimeSeries: metrics.TimeSeries{},
				},
			})

			if counter == 5 { // a fixed-arbitrary flush rate
				pop <- struct{}{}
				counter = 0
			}
		}
	}
}

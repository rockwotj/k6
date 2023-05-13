package expv2

import (
	"errors"
	"sync"
	"time"

	"go.k6.io/k6/metrics"
	"go.k6.io/k6/output"
)

type bucketQ struct {
	m       sync.Mutex
	buckets []timeBucket
}

// Buckets pop all the queued buckets.
func (q *bucketQ) Buckets() []timeBucket {
	q.m.Lock()
	defer q.m.Unlock()

	if len(q.buckets) < 1 {
		return nil
	}
	b := make([]timeBucket, len(q.buckets))
	copy(b, q.buckets)
	q.buckets = q.buckets[:0]
	return b
}

// Insert adds an item in the queue.
func (q *bucketQ) Push(b []timeBucket) {
	q.m.Lock()
	q.buckets = append(q.buckets, b...)
	q.m.Unlock()
}

type timeBucket struct {
	TimeSeries metrics.TimeSeries
	Time       time.Time
	Sink       metrics.Sink
}

type perSeriesBuckets map[metrics.TimeSeries]timeBucket

func newPerSeriesBuckets() perSeriesBuckets {
	return make(map[metrics.TimeSeries]timeBucket)
}

func (m perSeriesBuckets) AddSample(bucketID int64, s metrics.Sample, aggrPeriod int64) {
	b, ok := m[s.TimeSeries]
	if !ok {
		b = timeBucket{
			Time:       time.Unix(0, (bucketID*aggrPeriod)+(aggrPeriod/2)).Truncate(time.Microsecond),
			TimeSeries: s.TimeSeries,
			Sink:       newSink(s.Metric.Type),
		}
		m[s.TimeSeries] = b
	}
	b.Sink.Add(s)
}

type collector struct {
	buf   *output.SampleBuffer
	bq    bucketQ
	nowfn func() time.Time

	aggregationPeriod time.Duration
	waitPeriod        time.Duration

	// we should no longer have to handle metrics that have times long in the past. So instead of a
	// map, we can probably use a simple slice (or even an array!) as a ring buffer to store the
	// aggregation buckets. This should save us a some time, since it would make the lookups and WaitPeriod
	// checks basically O(1). And even if for some reason there are occasional metrics with past times that
	// don't fit in the chosen ring buffer size, we could just send them along to the buffer unaggregated
	aggrBuckets map[int64]perSeriesBuckets
}

func newCollector(buf *output.SampleBuffer, aggrPeriod, waitPeriod time.Duration) (*collector, error) {
	if aggrPeriod == 0 {
		return nil, errors.New("aggregation period is not allowed to be zero")
	}
	return &collector{
		buf:               buf,
		bq:                bucketQ{},
		nowfn:             time.Now,
		aggrBuckets:       make(map[int64]perSeriesBuckets),
		aggregationPeriod: aggrPeriod,
		waitPeriod:        waitPeriod,
	}, nil
}

// CollectSamples drain the buffer and collect all the samples.
func (c *collector) CollectSamples() {
	c.bucketing(c.buf.GetBufferedSamples())

	expired := c.expiredBuckets()
	if len(expired) > 0 {
		c.bq.Push(expired)
	}
}

func (c *collector) bucketID(t time.Time) int64 {
	return t.UnixNano() / int64(c.aggregationPeriod)
}

func (c *collector) bucketing(containers []metrics.SampleContainer) {
	// Distribute all newly buffered samples into related buckets
	for _, sampleContainer := range containers {
		samples := sampleContainer.GetSamples()

		for i := 0; i < len(samples); i++ {
			bucketID := c.bucketID(samples[i].Time)

			// Get or create a time bucket
			buckets, ok := c.aggrBuckets[bucketID]
			if !ok {
				buckets = newPerSeriesBuckets()
				c.aggrBuckets[bucketID] = buckets
			}

			buckets.AddSample(bucketID, samples[i], int64(c.aggregationPeriod))
		}
	}
}

func (c *collector) expiredBuckets() []timeBucket {
	// Still new buckets where we have to wait to accumulate
	// more samples before flushing
	//
	// TODO: Do we need it?
	// if the Cloud handles the merge is not really required
	bucketCutoffID := c.nowfn().Add(-c.waitPeriod).UnixNano() / int64(c.aggregationPeriod)

	// TODO: multiply it at least for the number of metrics?
	expired := make([]timeBucket, 0, len(c.aggrBuckets))

	// Handle all aggregation buckets older than bucketCutoffID
	for bucketID, seriesBuckets := range c.aggrBuckets {
		if bucketID > bucketCutoffID {
			continue
		}

		for _, bucket := range seriesBuckets {
			// TODO: is there a valid math reason to have a minimum
			// number of samples aggregated as we do in v1?
			// I mean o.config.AggregationMinSamples.Int64

			expired = append(expired, bucket)
		}
		delete(c.aggrBuckets, bucketID)
	}

	if len(expired) < 1 {
		return nil
	}
	return expired
}

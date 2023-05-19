package expv2

import (
	"errors"
	"sync"
	"time"

	"go.k6.io/k6/metrics"
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

type perSeriesBucket map[metrics.TimeSeries]timeBucket

func newPerSeriesBucket() perSeriesBucket {
	return make(map[metrics.TimeSeries]timeBucket)
}

type collector struct {
	bq    bucketQ
	nowfn func() time.Time

	aggregationPeriod time.Duration
	waitPeriod        time.Duration

	// we should no longer have to handle metrics that have times long in the past. So instead of a
	// map, we can probably use a simple slice (or even an array!) as a ring buffer to store the
	// aggregation buckets. This should save us a some time, since it would make the lookups and WaitPeriod
	// checks basically O(1). And even if for some reason there are occasional metrics with past times that
	// don't fit in the chosen ring buffer size, we could just send them along to the buffer unaggregated
	timeBuckets map[int64]perSeriesBucket
}

func newCollector(aggrPeriod, waitPeriod time.Duration) (*collector, error) {
	if aggrPeriod == 0 {
		return nil, errors.New("aggregation period is not allowed to be zero")
	}
	return &collector{
		bq:                bucketQ{},
		nowfn:             time.Now,
		timeBuckets:       make(map[int64]perSeriesBucket),
		aggregationPeriod: aggrPeriod,
		waitPeriod:        waitPeriod,
	}, nil
}

// CollectSamples drain the buffer and collect all the samples.
func (c *collector) CollectSamples(containers []metrics.SampleContainer) {
	// Distribute all newly buffered samples into related buckets
	for _, sampleContainer := range containers {
		samples := sampleContainer.GetSamples()

		for i := 0; i < len(samples); i++ {
			c.collectSample(samples[i])
		}
	}

	expired := c.expiredBuckets()
	if len(expired) > 0 {
		c.bq.Push(expired)
	}
}

func (c *collector) collectSample(s metrics.Sample) {
	bucketID := c.bucketID(s.Time)

	// Get or create a time bucket
	bucket, ok := c.timeBuckets[bucketID]
	if !ok {
		bucket = newPerSeriesBucket()
		c.timeBuckets[bucketID] = bucket
	}

	sink, ok := bucket[s.TimeSeries]
	if !ok {
		sink = timeBucket{
			Time:       time.Unix(0, (bucketID*int64(c.aggregationPeriod))+int64(c.aggregationPeriod/2)).Truncate(time.Microsecond),
			TimeSeries: s.TimeSeries,
			Sink:       newSink(s.Metric.Type),
		}
		bucket[s.TimeSeries] = sink
	}
	sink.Sink.Add(s)
}

func (c *collector) expiredBuckets() []timeBucket {
	// Still new buckets where we have to wait to accumulate
	// more samples before flushing
	bucketCutoffID := c.bucketCutoffID()

	// TODO: multiply it at least for the number of metrics?
	expired := make([]timeBucket, 0, len(c.timeBuckets))

	// Handle all aggregation buckets older than bucketCutoffID
	for bucketID, seriesBuckets := range c.timeBuckets {
		if bucketID > bucketCutoffID {
			continue
		}

		for _, bucket := range seriesBuckets {
			// TODO: is there a valid math reason to have a minimum
			// number of samples aggregated as we do in v1?
			// I mean o.config.AggregationMinSamples.Int64

			expired = append(expired, bucket)
		}
		delete(c.timeBuckets, bucketID)
	}

	if len(expired) < 1 {
		return nil
	}
	return expired
}

func (c *collector) bucketID(t time.Time) int64 {
	return t.UnixNano() / int64(c.aggregationPeriod)
}

func (c *collector) bucketCutoffID() int64 {
	return c.nowfn().Add(-c.waitPeriod).UnixNano() / int64(c.aggregationPeriod)
}

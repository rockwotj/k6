package cloud_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.k6.io/k6/output"
	"go.k6.io/k6/output/cloud"
)

func TestNewVersionError(t *testing.T) {
	p := output.Params{
		Environment: map[string]string{
			"K6_CLOUD_API_VERSION": "3",
		},
	}
	o, err := cloud.New(p)
	require.ErrorContains(t, err, "v3 is an unexpected version")
	assert.Nil(t, o)
}

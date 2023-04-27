package cloud

import (
	"fmt"

	"go.k6.io/k6/cloudapi"
	"go.k6.io/k6/output"
	cloudv2 "go.k6.io/k6/output/cloud/expv2"
	cloudv1 "go.k6.io/k6/output/cloud/v1"
)

// TODO: the following architecture will be removed
// when we will drop the old version V1.

type apiVersion int64

const (
	apiVersionUndefined apiVersion = iota
	apiVersion1
	apiVersion2
)

// gateway wraps a versioned Cloud Output implementation.
type gateway struct {
	VersionedOutput
}

// Verify that Output implements the wanted interfaces
type VersionedOutput interface {
	output.WithStopWithTestError
	output.WithThresholds
	output.WithTestRunStop
}

var _ output.Output = &gateway{}

func New(params output.Params) (output.Output, error) {
	conf, err := cloudapi.GetConsolidatedConfig(
		params.JSONConfig, params.Environment, params.ConfigArgument, params.ScriptOptions.External)
	if err != nil {
		return nil, err
	}

	r := gateway{}

	switch conf.APIVersion.Int64 {
	case int64(apiVersion1):
		r.VersionedOutput, err = cloudv1.New(params)
	case int64(apiVersion2):
		r.VersionedOutput, err = cloudv2.New(params)
	default:
		err = fmt.Errorf("v%d is an unexpected version", conf.APIVersion.Int64)
	}

	if err != nil {
		return nil, err
	}

	return r, nil
}

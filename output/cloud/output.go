package cloud

import (
	"fmt"

	"go.k6.io/k6/cloudapi"
	"go.k6.io/k6/output"
	v1 "go.k6.io/k6/output/cloud/v1"
)

// TODO: the following architecture will be removed
// when we will drop the old version V1.

type apiVersion int64

const (
	apiVersionUndefined apiVersion = iota
	apiVersion1
	// apiVersion2 // TODO: add version 2
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
		r.VersionedOutput, err = v1.New(params)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("v%d is an unexpected version", conf.APIVersion.Int64)
	}

	return r, nil
}

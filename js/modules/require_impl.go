package modules

import (
	"errors"
	"net/url"
	"strings"

	"github.com/dop251/goja"
	"go.k6.io/k6/loader"
)

// OldRequireImpl is an implementation of require that is the old k6 behaviours
// It happens to not be commonJS compatible as it loads relative to the currentl required file,
// instead of relative to the file the `require()` is written in.
type OldRequireImpl struct {
	vu                      VU
	modules                 *ModuleSystem
	currentlyRequiredModule *url.URL
}

// NewOldRequireImpl creates a new NewOldRequireImpl
func NewOldRequireImpl(vu VU, ms *ModuleSystem, pwd url.URL) *OldRequireImpl {
	return &OldRequireImpl{
		vu:                      vu,
		modules:                 ms,
		currentlyRequiredModule: &pwd,
	}
}

// Require is the actual call that implements require
func (r *OldRequireImpl) Require(specifier string) (*goja.Object, error) {
	// TODO remove this in the future when we address https://github.com/grafana/k6/issues/2674
	// This is currently needed as each time require is called we need to record it's new pwd
	// to be used if a require *or* open is used within the file as they are relative to the
	// latest call to require.
	// This is *not* the actual require behaviour defined in commonJS as it is actually always relative
	// to the file it is in. This is unlikely to be an issue but this code is here to keep backwards
	// compatibility *for now*.
	// With native ESM this won't even be possible as `require` might not be called - instead an import
	// might be used in which case we won't be able to be doing this hack. In that case we either will
	// need some goja specific helper or to use stack traces as goja_nodejs does.
	currentPWD := r.currentlyRequiredModule
	if specifier != "k6" && !strings.HasPrefix(specifier, "k6/") {
		defer func() {
			r.currentlyRequiredModule = currentPWD
		}()
		// In theory we can give that downwards, but this makes the code more tightly coupled
		// plus as explained above this will be removed in the future so the code reflects more
		// closely what will be needed then
		fileURL, err := loader.Resolve(r.currentlyRequiredModule, specifier)
		if err != nil {
			return nil, err
		}
		r.currentlyRequiredModule = loader.Dir(fileURL)
	}

	if specifier == "" {
		return nil, errors.New("require() can't be used with an empty specifier")
	}

	return r.modules.Require(currentPWD, specifier)
}

// CurrentlyRequiredModule returns the module that is currently being required.
// It is mostly used for old and somewhat buggy behaviour of the `open` call
func (r *OldRequireImpl) CurrentlyRequiredModule() url.URL {
	return *r.currentlyRequiredModule
}

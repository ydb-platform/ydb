// Code created by gotmpl. DO NOT MODIFY.
// source: internal/shared/matchers/expecter.go.tmpl

// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package matchers // import "go.opentelemetry.io/otel/internal/matchers"

import (
	"testing"
)

type Expecter struct {
	t *testing.T
}

func NewExpecter(t *testing.T) *Expecter {
	return &Expecter{
		t: t,
	}
}

func (a *Expecter) Expect(actual interface{}) *Expectation {
	return &Expectation{
		t:      a.t,
		actual: actual,
	}
}

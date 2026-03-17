# Copyright The OpenTracing Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
import pytest
import mock
import opentracing


def teardown_function(function):
    opentracing._reset_global_tracer()


def test_opentracing_tracer():
    assert opentracing.tracer is opentracing.global_tracer()
    assert isinstance(opentracing.global_tracer(), opentracing.Tracer)


def test_is_global_tracer_registered():
    assert opentracing.is_global_tracer_registered() is False


def test_set_global_tracer():
    tracer = mock.Mock()
    opentracing.set_global_tracer(tracer)
    assert opentracing.global_tracer() is tracer
    assert opentracing.is_global_tracer_registered()

    # Register another value.
    tracer = mock.Mock()
    opentracing.set_global_tracer(tracer)
    assert opentracing.global_tracer() is tracer
    assert opentracing.is_global_tracer_registered()


def test_register_none():
    with pytest.raises(ValueError):
        opentracing.set_global_tracer(None)

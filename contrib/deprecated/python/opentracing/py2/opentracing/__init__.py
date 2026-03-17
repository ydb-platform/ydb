# Copyright The OpenTracing Authors
# Copyright Uber Technologies, Inc
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
from .span import Span  # noqa
from .span import SpanContext  # noqa
from .scope import Scope  # noqa
from .scope_manager import ScopeManager  # noqa
from .tracer import child_of  # noqa
from .tracer import follows_from  # noqa
from .tracer import Reference  # noqa
from .tracer import ReferenceType  # noqa
from .tracer import Tracer  # noqa
from .tracer import start_child_span  # noqa
from .propagation import Format  # noqa
from .propagation import InvalidCarrierException  # noqa
from .propagation import SpanContextCorruptedException  # noqa
from .propagation import UnsupportedFormatException  # noqa

# Global variable that should be initialized to an instance of real tracer.
# Note: it should be accessed via 'opentracing.tracer', not via
# 'from opentracing import tracer', the latter seems to take a copy.
# DEPRECATED, use global_tracer() and set_global_tracer() instead.
tracer = Tracer()
is_tracer_registered = False


def global_tracer():
    """Returns the global tracer.
    The default value is an instance of :class:`opentracing.Tracer`

    :rtype: :class:`Tracer`
    :return: The global tracer instance.
    """
    return tracer


def set_global_tracer(value):
    """Sets the global tracer.
    It is an error to pass ``None``.

    :param value: the :class:`Tracer` used as global instance.
    :type value: :class:`Tracer`
    """
    if value is None:
        raise ValueError('The global Tracer tracer cannot be None')

    global tracer, is_tracer_registered
    tracer = value
    is_tracer_registered = True


def is_global_tracer_registered():
    """Indicates if a global tracer has been registered.

    :rtype: :value:bool
    :return: True if a global tracer has been registered, otherwise False.
    """
    return is_tracer_registered


def _reset_global_tracer():
    """Reset any previously registered tracer. Intended for internal usage."""

    global tracer, is_tracer_registered
    tracer = Tracer()
    is_tracer_registered = False

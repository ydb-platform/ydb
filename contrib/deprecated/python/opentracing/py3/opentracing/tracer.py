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
from collections import namedtuple
from .span import Span
from .span import SpanContext
from .scope import Scope
from .scope_manager import ScopeManager
from .propagation import Format, UnsupportedFormatException


class Tracer(object):
    """Tracer is the entry point API between instrumentation code and the
    tracing implementation.

    This implementation both defines the public Tracer API, and provides
    a default no-op behavior.
    """

    _supported_formats = [Format.TEXT_MAP, Format.BINARY, Format.HTTP_HEADERS]

    def __init__(self, scope_manager=None):
        self._scope_manager = ScopeManager() if scope_manager is None \
                else scope_manager
        self._noop_span_context = SpanContext()
        self._noop_span = Span(tracer=self, context=self._noop_span_context)
        self._noop_scope = Scope(self._scope_manager, self._noop_span)

    @property
    def scope_manager(self):
        """Provides access to the current :class:`~opentracing.ScopeManager`.

        :rtype: :class:`~opentracing.ScopeManager`
        """
        return self._scope_manager

    @property
    def active_span(self):
        """Provides access to the the active :class:`Span`. This is a shorthand for
        :attr:`Tracer.scope_manager.active.span`, and ``None`` will be
        returned if :attr:`Scope.span` is ``None``.

        :rtype: :class:`~opentracing.Span`
        :return: the active :class:`Span`.
        """
        scope = self._scope_manager.active
        return None if scope is None else scope.span

    def start_active_span(self,
                          operation_name,
                          child_of=None,
                          references=None,
                          tags=None,
                          start_time=None,
                          ignore_active_span=False,
                          finish_on_close=True):
        """Returns a newly started and activated :class:`Scope`.

        The returned :class:`Scope` supports with-statement contexts. For
        example::

            with tracer.start_active_span('...') as scope:
                scope.span.set_tag('http.method', 'GET')
                do_some_work()
            # Span.finish() is called as part of scope deactivation through
            # the with statement.

        It's also possible to not finish the :class:`Span` when the
        :class:`Scope` context expires::

            with tracer.start_active_span('...',
                                          finish_on_close=False) as scope:
                scope.span.set_tag('http.method', 'GET')
                do_some_work()
            # Span.finish() is not called as part of Scope deactivation as
            # `finish_on_close` is `False`.

        :param operation_name: name of the operation represented by the new
            :class:`Span` from the perspective of the current service.
        :type operation_name: str

        :param child_of: (optional) a :class:`Span` or :class:`SpanContext`
            instance representing the parent in a REFERENCE_CHILD_OF reference.
            If specified, the `references` parameter must be omitted.
        :type child_of: Span or SpanContext

        :param references: (optional) references that identify one or more
            parent :class:`SpanContext`\\ s. (See the Reference documentation
            for detail).
        :type references: :obj:`list` of :class:`Reference`

        :param tags: an optional dictionary of :class:`Span` tags. The caller
            gives up ownership of that dictionary, because the :class:`Tracer`
            may use it as-is to avoid extra data copying.
        :type tags: dict

        :param start_time: an explicit :class:`Span` start time as a unix
            timestamp per :meth:`time.time()`.
        :type start_time: float

        :param ignore_active_span: (optional) an explicit flag that ignores
            the current active :class:`Scope` and creates a root :class:`Span`.
        :type ignore_active_span: bool

        :param finish_on_close: whether :class:`Span` should automatically be
            finished when :meth:`Scope.close()` is called.
        :type finish_on_close: bool

        :rtype: Scope
        :return: a :class:`Scope`, already registered via the
            :class:`ScopeManager`.
        """
        return self._noop_scope

    def start_span(self,
                   operation_name=None,
                   child_of=None,
                   references=None,
                   tags=None,
                   start_time=None,
                   ignore_active_span=False):
        """Starts and returns a new :class:`Span` representing a unit of work.


        Starting a root :class:`Span` (a :class:`Span` with no causal
        references)::

            tracer.start_span('...')


        Starting a child :class:`Span` (see also :meth:`start_child_span()`)::

            tracer.start_span(
                '...',
                child_of=parent_span)


        Starting a child :class:`Span` in a more verbose way::

            tracer.start_span(
                '...',
                references=[opentracing.child_of(parent_span)])


        :param operation_name: name of the operation represented by the new
            :class:`Span` from the perspective of the current service.
        :type operation_name: str

        :param child_of: (optional) a :class:`Span` or :class:`SpanContext`
            representing the parent in a REFERENCE_CHILD_OF reference.  If
            specified, the `references` parameter must be omitted.
        :type child_of: Span or SpanContext

        :param references: (optional) references that identify one or more
            parent :class:`SpanContext`\\ s. (See the Reference documentation
            for detail).
        :type references: :obj:`list` of :class:`Reference`

        :param tags: an optional dictionary of :class:`Span` tags. The caller
            gives up ownership of that dictionary, because the :class:`Tracer`
            may use it as-is to avoid extra data copying.
        :type tags: dict

        :param start_time: an explicit Span start time as a unix timestamp per
            :meth:`time.time()`
        :type start_time: float

        :param ignore_active_span: an explicit flag that ignores the current
            active :class:`Scope` and creates a root :class:`Span`.
        :type ignore_active_span: bool

        :rtype: Span
        :return: an already-started :class:`Span` instance.
        """
        return self._noop_span

    def inject(self, span_context, format, carrier):
        """Injects `span_context` into `carrier`.

        The type of `carrier` is determined by `format`. See the
        :class:`Format` class/namespace for the built-in OpenTracing formats.

        Implementations *must* raise :exc:`UnsupportedFormatException` if
        `format` is unknown or disallowed.

        :param span_context: the :class:`SpanContext` instance to inject
        :type span_context: SpanContext

        :param format: a python object instance that represents a given
            carrier format. `format` may be of any type, and `format` equality
            is defined by python ``==`` equality.
        :type format: Format
        :param carrier: the format-specific carrier object to inject into
        """
        if format in Tracer._supported_formats:
            return
        raise UnsupportedFormatException(format)

    def extract(self, format, carrier):
        """Returns a :class:`SpanContext` instance extracted from a `carrier` of the
        given `format`, or ``None`` if no such :class:`SpanContext` could be
        found.

        The type of `carrier` is determined by `format`. See the
        :class:`Format` class/namespace for the built-in OpenTracing formats.

        Implementations *must* raise :exc:`UnsupportedFormatException` if
        `format` is unknown or disallowed.

        Implementations may raise :exc:`InvalidCarrierException`,
        :exc:`SpanContextCorruptedException`, or implementation-specific errors
        if there are problems with `carrier`.


        :param format: a python object instance that represents a given
            carrier format. `format` may be of any type, and `format` equality
            is defined by python ``==`` equality.

        :param carrier: the format-specific carrier object to extract from

        :rtype: SpanContext
        :return: a :class:`SpanContext` extracted from `carrier` or ``None`` if
            no such :class:`SpanContext` could be found.
        """
        if format in Tracer._supported_formats:
            return self._noop_span_context
        raise UnsupportedFormatException(format)


class ReferenceType(object):
    """A namespace for OpenTracing reference types.

    See http://opentracing.io/spec for more detail about references,
    reference types, and CHILD_OF and FOLLOWS_FROM in particular.
    """
    CHILD_OF = 'child_of'
    FOLLOWS_FROM = 'follows_from'


# We use namedtuple since references are meant to be immutable.
# We subclass it to expose a standard docstring.
class Reference(namedtuple('Reference', ['type', 'referenced_context'])):
    """A Reference pairs a reference type with a referenced :class:`SpanContext`.

    References are used by :meth:`Tracer.start_span()` to describe the
    relationships between :class:`Span`\\ s.

    :class:`Tracer` implementations must ignore references where
    referenced_context is ``None``.  This behavior allows for simpler code when
    an inbound RPC request contains no tracing information and as a result
    :meth:`Tracer.extract()` returns ``None``::

        parent_ref = tracer.extract(opentracing.HTTP_HEADERS, request.headers)
        span = tracer.start_span(
            'operation', references=child_of(parent_ref)
        )

    See :meth:`child_of` and :meth:`follows_from` helpers for creating these
    references.
    """
    pass


def child_of(referenced_context=None):
    """child_of is a helper that creates CHILD_OF References.

    :param referenced_context: the (causal parent) :class:`SpanContext` to
        reference. If ``None`` is passed, this reference must be ignored by
        the :class:`Tracer`.
    :type referenced_context: SpanContext

    :rtype: Reference
    :return: A reference suitable for ``Tracer.start_span(...,
        references=...)``
    """
    return Reference(
        type=ReferenceType.CHILD_OF,
        referenced_context=referenced_context)


def follows_from(referenced_context=None):
    """follows_from is a helper that creates FOLLOWS_FROM References.

    :param referenced_context: the (causal parent) :class:`SpanContext` to
        reference. If ``None`` is passed, this reference must be ignored by the
        :class:`Tracer`.
    :type referenced_context: SpanContext

    :rtype: Reference
    :return: A Reference suitable for ``Tracer.start_span(...,
        references=...)``
    """
    return Reference(
        type=ReferenceType.FOLLOWS_FROM,
        referenced_context=referenced_context)


def start_child_span(parent_span, operation_name, tags=None, start_time=None):
    """A shorthand method that starts a `child_of` :class:`Span` for a given
    parent :class:`Span`.

    Equivalent to calling::

        parent_span.tracer().start_span(
            operation_name,
            references=opentracing.child_of(parent_span.context),
            tags=tags,
            start_time=start_time)

    :param parent_span: the :class:`Span` which will act as the parent in the
        returned :class:`Span`\\ s child_of reference.
    :type parent_span: Span

    :param operation_name: the operation name for the child :class:`Span`
        instance
    :type operation_name: str

    :param tags: optional dict of :class:`Span` tags. The caller gives up
        ownership of that dict, because the :class:`Tracer` may use it as-is to
        avoid extra data copying.
    :type tags: dict

    :param start_time: an explicit :class:`Span` start time as a unix timestamp
        per :meth:`time.time()`.
    :type start_time: float

    :rtype: Span
    :return: an already-started :class:`Span` instance.
    """
    return parent_span.tracer.start_span(
        operation_name=operation_name,
        child_of=parent_span,
        tags=tags,
        start_time=start_time
    )

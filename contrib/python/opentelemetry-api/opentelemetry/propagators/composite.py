# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0
import collections.abc
import logging

from typing_extensions import deprecated

from opentelemetry.context.context import Context
from opentelemetry.propagators import textmap

logger = logging.getLogger(__name__)


class CompositePropagator(textmap.TextMapPropagator):
    """CompositePropagator provides a mechanism for combining multiple
    propagators into a single one.

    Args:
        propagators: the list of propagators to use
    """

    def __init__(
        self, propagators: collections.abc.Sequence[textmap.TextMapPropagator]
    ) -> None:
        self._propagators = propagators

    def extract(
        self,
        carrier: textmap.CarrierT,
        context: Context | None = None,
        getter: textmap.Getter[textmap.CarrierT] = textmap.default_getter,
    ) -> Context:
        """Run each of the configured propagators with the given context and carrier.
        Propagators are run in the order they are configured, if multiple
        propagators write the same context key, the propagator later in the list
        will override previous propagators.

        See `opentelemetry.propagators.textmap.TextMapPropagator.extract`
        """
        for propagator in self._propagators:
            context = propagator.extract(carrier, context, getter=getter)
        return context  # type: ignore

    def inject(
        self,
        carrier: textmap.CarrierT,
        context: Context | None = None,
        setter: textmap.Setter[textmap.CarrierT] = textmap.default_setter,
    ) -> None:
        """Run each of the configured propagators with the given context and carrier.
        Propagators are run in the order they are configured, if multiple
        propagators write the same carrier key, the propagator later in the list
        will override previous propagators.

        See `opentelemetry.propagators.textmap.TextMapPropagator.inject`
        """
        for propagator in self._propagators:
            propagator.inject(carrier, context, setter=setter)

    @property
    def fields(self) -> set[str]:
        """Returns a set with the fields set in `inject`.

        See
        `opentelemetry.propagators.textmap.TextMapPropagator.fields`
        """
        composite_fields = set()

        for propagator in self._propagators:
            for field in propagator.fields:
                composite_fields.add(field)

        return composite_fields


@deprecated(
    "You should use CompositePropagator. Deprecated since version 1.2.0."
)
class CompositeHTTPPropagator(CompositePropagator):
    """CompositeHTTPPropagator provides a mechanism for combining multiple
    propagators into a single one.
    """

"""
Resource for sharing homogeneous matter between processes, either continuous
(like water) or discrete (like apples).

A :class:`Container` can be used to model the fuel tank of a gasoline station.
Tankers increase and refuelled cars decrease the amount of gas in the station's
fuel tanks.

"""
from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

from simpy.core import BoundClass, Environment
from simpy.resources import base

ContainerAmount = Union[int, float]


class ContainerPut(base.Put):
    """Request to put *amount* of matter into the *container*. The request will
    be triggered once there is enough space in the *container* available.

    Raise a :exc:`ValueError` if ``amount <= 0``.

    """

    def __init__(self, container: Container, amount: ContainerAmount):
        if amount <= 0:
            raise ValueError(f'amount(={amount}) must be > 0.')
        self.amount = amount
        """The amount of matter to be put into the container."""

        super().__init__(container)


class ContainerGet(base.Get):
    """Request to get *amount* of matter from the *container*. The request will
    be triggered once there is enough matter available in the *container*.

    Raise a :exc:`ValueError` if ``amount <= 0``.

    """

    def __init__(self, container: Container, amount: ContainerAmount):
        if amount <= 0:
            raise ValueError(f'amount(={amount}) must be > 0.')
        self.amount = amount
        """The amount of matter to be taken out of the container."""

        super().__init__(container)


class Container(base.BaseResource):
    """Resource containing up to *capacity* of matter which may either be
    continuous (like water) or discrete (like apples). It supports requests to
    put or get matter into/from the container.

    The *env* parameter is the :class:`~simpy.core.Environment` instance the
    container is bound to.

    The *capacity* defines the size of the container. By default, a container
    is of unlimited size. The initial amount of matter is specified by *init*
    and defaults to ``0``.

    Raise a :exc:`ValueError` if ``capacity <= 0``, ``init < 0`` or
    ``init > capacity``.

    """

    def __init__(
        self,
        env: Environment,
        capacity: ContainerAmount = float('inf'),
        init: ContainerAmount = 0,
    ):
        if capacity <= 0:
            raise ValueError('"capacity" must be > 0.')
        if init < 0:
            raise ValueError('"init" must be >= 0.')
        if init > capacity:
            raise ValueError('"init" must be <= "capacity".')

        super().__init__(env, capacity)

        self._level = init

    @property
    def level(self) -> ContainerAmount:
        """The current amount of the matter in the container."""
        return self._level

    if TYPE_CHECKING:

        def put(  # type: ignore[override]
            self, amount: ContainerAmount
        ) -> ContainerPut:
            """Request to put *amount* of matter into the container."""
            return ContainerPut(self, amount)

        def get(  # type: ignore[override]
            self, amount: ContainerAmount
        ) -> ContainerGet:
            """Request to get *amount* of matter out of the container."""
            return ContainerGet(self, amount)

    else:
        put = BoundClass(ContainerPut)
        get = BoundClass(ContainerGet)

    def _do_put(self, event: ContainerPut) -> Optional[bool]:
        if self._capacity - self._level >= event.amount:
            self._level += event.amount
            event.succeed()
            return True
        else:
            return None

    def _do_get(self, event: ContainerGet) -> Optional[bool]:
        if self._level >= event.amount:
            self._level -= event.amount
            event.succeed()
            return True
        else:
            return None

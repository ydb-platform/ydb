#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from typing import Any, Dict, Tuple

from pysnmp.proto.api import v2c
from pysnmp.smi import builder, view
from pysnmp.smi.rfc1902 import NotificationType, ObjectIdentity, ObjectType

__all__ = ["CommandGeneratorVarBinds", "NotificationOriginatorVarBinds"]


def is_end_of_mib(var_binds):  # noqa: N816
    """
    Check if the given variable bindings indicate the end of the MIB.

    Parameters:
    var_binds (list): A list of variable bindings.

    Returns:
    bool: True if it is the end of the MIB, False otherwise.
    """
    return not v2c.apiPDU.get_next_varbinds(var_binds)[1]


class MibViewControllerManager:
    @staticmethod
    def get_mib_view_controller(userCache):
        try:
            mibViewController = userCache["mibViewController"]

        except KeyError:
            mibViewController = view.MibViewController(builder.MibBuilder())
            userCache["mibViewController"] = mibViewController

        return mibViewController


class CommandGeneratorVarBinds(MibViewControllerManager):
    """Var-binds processor for Command Generator."""

    def make_varbinds(
        self, userCache: Dict[str, Any], varBinds: Tuple[ObjectType, ...]
    ) -> Tuple[ObjectType, ...]:
        """Return a tuple of ObjectType instances."""
        mibViewController = self.get_mib_view_controller(userCache)

        resolvedVarBinds = []

        for varBind in varBinds:
            if isinstance(varBind, ObjectType):
                pass

            elif isinstance(varBind[0], ObjectIdentity):
                varBind = ObjectType(*varBind)

            elif isinstance(varBind[0][0], tuple):  # legacy
                varBind = ObjectType(
                    ObjectIdentity(varBind[0][0][0], varBind[0][0][1], *varBind[0][1:]),
                    varBind[1],
                )

            else:
                varBind = ObjectType(ObjectIdentity(varBind[0]), varBind[1])

            resolvedVarBinds.append(
                varBind.resolve_with_mib(mibViewController, ignoreErrors=False)
            )

        return tuple(resolvedVarBinds)

    def unmake_varbinds(
        self,
        userCache: Dict[str, Any],
        varBinds: Tuple[ObjectType, ...],
        lookupMib=True,
    ) -> Tuple[ObjectType, ...]:
        """Return a tuple of ObjectType instances."""
        if lookupMib:
            mibViewController = self.get_mib_view_controller(userCache)
            varBinds = tuple(
                ObjectType(ObjectIdentity(x[0]), x[1]).resolve_with_mib(
                    mibViewController
                )
                for x in varBinds
            )

        return varBinds


class NotificationOriginatorVarBinds(MibViewControllerManager):
    """Var-binds processor for Notification Originator."""

    def make_varbinds(
        self, userCache: Dict[str, Any], varBinds: "tuple[NotificationType, ...]"
    ) -> "tuple[ObjectType, ...]":
        """Return a tuple of ObjectType instances."""
        mibViewController = self.get_mib_view_controller(userCache)

        # TODO: this shouldn't be needed
        if isinstance(varBinds, NotificationType):
            return varBinds.resolve_with_mib(
                mibViewController, ignoreErrors=False
            ).to_varbinds()

        resolvedVarBinds: "list[ObjectType]" = []

        for varBind in varBinds:
            if isinstance(varBind, NotificationType):
                resolvedVarBinds.extend(
                    varBind.resolve_with_mib(
                        mibViewController, ignoreErrors=False
                    ).to_varbinds()
                )
                continue

            if isinstance(varBind, ObjectType):
                pass

            elif isinstance(varBind[0], ObjectIdentity):
                varBind = ObjectType(*varBind)

            else:
                varBind = ObjectType(ObjectIdentity(varBind[0]), varBind[1])

            resolvedVarBinds.append(
                varBind.resolve_with_mib(mibViewController, ignoreErrors=False)
            )

        return tuple(resolvedVarBinds)

    def unmake_varbinds(
        self,
        userCache: Dict[str, Any],
        varBinds: "tuple[ObjectType, ...]",
        lookupMib=False,
    ) -> "tuple[ObjectType, ...]":
        """Return a tuple of ObjectType instances."""
        if lookupMib:
            mibViewController = self.get_mib_view_controller(userCache)
            varBinds = tuple(
                ObjectType(ObjectIdentity(x[0]), x[1]).resolve_with_mib(
                    mibViewController
                )
                for x in varBinds
            )
        return varBinds

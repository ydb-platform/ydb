import abc
from typing import ClassVar, ContextManager, List, Optional

import click

from pyhanko.cli._ctx import CLIContext
from pyhanko.sign import Signer

__all__ = [
    'SigningCommandPlugin',
    'register_signing_plugin',
    'CLIContext',
    'SIGNING_PLUGIN_REGISTRY',
    'SIGNING_PLUGIN_ENTRY_POINT_GROUP',
]


class SigningCommandPlugin(abc.ABC):
    """

    .. versionadded:: 0.18.0

    Interface for integrating custom, user-supplied
    signers into the pyHanko CLI as subcommands of ``addsig``.

    Implementations are discovered through the ``pyhanko.cli_plugin.signing``
    package entry point. Such entry points can be registered in
    ``pyproject.toml`` as follows:

    .. code-block:: toml

       [project.entry-points."pyhanko.cli_plugin.signing"]
       your_plugin = "some_package.path.to.module:SomePluginClass"

    Subclasses exposed as entry points are required to have a no-arguments
    ``__init__`` method.

    .. warning::
        This is an incubating feature. API adjustments are still possible.

    .. warning::
        Plugin support requires Python 3.8 or later.
    """

    subcommand_name: ClassVar[str]
    """
    The name of the subcommand for the plugin.
    """

    help_summary: ClassVar[str]
    """
    A short description of the plugin for use in the ``--help`` output.
    """

    unavailable_message: ClassVar[Optional[str]] = None
    """
    Message to display if the plugin is unavailable.
    """

    def click_options(self) -> List[click.Option]:
        """
        The list of ``click`` options for your custom command.
        """
        raise NotImplementedError

    def click_extra_arguments(self) -> List[click.Argument]:
        """
        The list of ``click`` arguments for your custom command.
        """
        return []

    def is_available(self) -> bool:
        """
        A hook to determine whether your plugin is available
        or not (e.g. based on the availability of certain dependencies).
        This should not depend on the pyHanko configuration, but may query
        system information in other ways as appropriate.

        The default is to always report the plugin as available.

        :return:
            return ``True`` if the plugin is available, else ``False``
        """
        return True

    def create_signer(
        self, context: CLIContext, **kwargs
    ) -> ContextManager[Signer]:
        """
        Instantiate a context manager that creates and potentially
        also implements a deallocator for a :class:`.Signer` object.

        :param context:
            The active :class:`.CLIContext`.
        :param kwargs:
            All keyword arguments processed by ``click`` through the CLI,
            resulting from :meth:`click_options` and
            :meth:`click_extra_arguments`.
        :return:
            A context manager that manages the lifecycle for a :class:`.Signer`.
        """
        raise NotImplementedError


SIGNING_PLUGIN_REGISTRY: List[SigningCommandPlugin] = []
SIGNING_PLUGIN_ENTRY_POINT_GROUP = 'pyhanko.cli_plugin.signing'


def register_signing_plugin(cls):
    """
    Manually put a plugin into the signing plugin registry.

    :param cls:
        A plugin class.
    :return:
        The same class.
    """
    SIGNING_PLUGIN_REGISTRY.append(cls())
    return cls

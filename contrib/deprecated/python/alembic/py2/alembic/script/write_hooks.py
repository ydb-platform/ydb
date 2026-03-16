import shlex
import subprocess
import sys

from .. import util
from ..util import compat


REVISION_SCRIPT_TOKEN = "REVISION_SCRIPT_FILENAME"

_registry = {}


def register(name):
    """A function decorator that will register that function as a write hook.

    See the documentation linked below for an example.

    .. versionadded:: 1.2.0

    .. seealso::

        :ref:`post_write_hooks_custom`


    """

    def decorate(fn):
        _registry[name] = fn

    return decorate


def _invoke(name, revision, options):
    """Invokes the formatter registered for the given name.

    :param name: The name of a formatter in the registry
    :param revision: A :class:`.MigrationRevision` instance
    :param options: A dict containing kwargs passed to the
        specified formatter.
    :raises: :class:`alembic.util.CommandError`
    """
    try:
        hook = _registry[name]
    except KeyError as ke:
        compat.raise_(
            util.CommandError("No formatter with name '%s' registered" % name),
            from_=ke,
        )
    else:
        return hook(revision, options)


def _run_hooks(path, hook_config):
    """Invoke hooks for a generated revision."""

    from .base import _split_on_space_comma

    names = _split_on_space_comma.split(hook_config.get("hooks", ""))

    for name in names:
        if not name:
            continue
        opts = {
            key[len(name) + 1 :]: hook_config[key]
            for key in hook_config
            if key.startswith(name + ".")
        }
        opts["_hook_name"] = name
        try:
            type_ = opts["type"]
        except KeyError as ke:
            compat.raise_(
                util.CommandError(
                    "Key %s.type is required for post write hook %r"
                    % (name, name)
                ),
                from_=ke,
            )
        else:
            util.status(
                'Running post write hook "%s"' % name,
                _invoke,
                type_,
                path,
                opts,
                newline=True,
            )


def _parse_cmdline_options(cmdline_options_str, path):
    """Parse options from a string into a list.

    Also substitutes the revision script token with the actual filename of
    the revision script.

    If the revision script token doesn't occur in the options string, it is
    automatically prepended.
    """
    if REVISION_SCRIPT_TOKEN not in cmdline_options_str:
        cmdline_options_str = REVISION_SCRIPT_TOKEN + " " + cmdline_options_str
    cmdline_options_list = shlex.split(
        cmdline_options_str, posix=compat.is_posix
    )
    cmdline_options_list = [
        option.replace(REVISION_SCRIPT_TOKEN, path)
        for option in cmdline_options_list
    ]
    return cmdline_options_list


@register("console_scripts")
def console_scripts(path, options):
    import pkg_resources

    try:
        entrypoint_name = options["entrypoint"]
    except KeyError as ke:
        compat.raise_(
            util.CommandError(
                "Key %s.entrypoint is required for post write hook %r"
                % (options["_hook_name"], options["_hook_name"])
            ),
            from_=ke,
        )
    iter_ = pkg_resources.iter_entry_points("console_scripts", entrypoint_name)
    impl = next(iter_)
    cwd = options.get("cwd", None)
    cmdline_options_str = options.get("options", "")
    cmdline_options_list = _parse_cmdline_options(cmdline_options_str, path)

    subprocess.run(
        [
            sys.executable,
            "-c",
            "import %s; %s()"
            % (impl.module_name, ".".join((impl.module_name,) + impl.attrs)),
        ]
        + cmdline_options_list,
        cwd=cwd,
    )

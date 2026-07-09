import _common

import ymake
from ymake import macro, Unit


@macro
def CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(unit: Unit, *args: tuple[str, ...]):
    """
    @usage: CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(ProjectRoot)

    Sets cmake target name to the module path relative to ProjectRoot with '/' replaced by '-',
    without changing the name of the output artefact (see CMAKE_EXPORTED_TARGET_NAME).
    Use it to resolve target name conflicts between same-named modules when a project subtree
    is exported to cmake. For example, a PROGRAM(app) module at proj/foo/bar/app
    with CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(proj) is exported as cmake target
    foo-bar-app still building artefact named app.

    The module must be located strictly inside ProjectRoot.
    """
    if len(args) != 1:
        ymake.report_configure_error(
            '[[imp]]CMAKE_EXPORTED_TARGET_NAME_FROM_PATH[[rst]] expects exactly one argument: a project root path'
        )
        return
    module_dir = _common.strip_roots(unit.path())
    root = args[0].strip('/')
    if not root or not module_dir.startswith(root + '/'):
        ymake.report_configure_error(
            "[[imp]]CMAKE_EXPORTED_TARGET_NAME_FROM_PATH[[rst]]: module '[[imp]]{}[[rst]]' is not strictly inside '[[unimp]]{}[[rst]]'".format(
                module_dir, args[0]
            )
        )
        return
    unit.oncmake_exported_target_name([module_dir[len(root) + 1 :].replace('/', '-')])

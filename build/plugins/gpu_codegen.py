import posixpath

import _common

from ymake import macro, Unit


def environment_resource_name(unit: Unit, path: str):
    path = posixpath.normpath(_common.rootrel_arc_src(path, unit))
    return "GPU_CODEGEN_ENV_" + _common.pathid(path)


@macro
def GPU_CODEGEN_ENVIRONMENT(unit: Unit, resource_uri: str):
    unit.ondeclare_external_resource([environment_resource_name(unit, unit.path()), resource_uri])

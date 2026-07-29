from ymake import macro, Unit


@macro
def FILES(unit: Unit, *args: tuple[str, ...]):
    args = list(args)
    for arg in args:
        if not arg.startswith('${ARCADIA_BUILD_ROOT}'):
            unit.oncopy_file([arg, arg])

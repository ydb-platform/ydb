from ymake import macro, Unit


@macro
def LJ_ARCHIVE(unit: Unit, *args: tuple[str, ...]):
    """
    @usage: LJ_ARCHIVE(NAME Name LuaFiles...)
    Precompile .lua files using LuaJIT and archive both sources and results using sources names as keys
    """

    def iter_luas(data):
        for a in data:
            if a.endswith('.lua'):
                yield a

    def iter_objs(data):
        for a in data:
            s = a[:-3] + 'raw'
            unit.on_luajit_objdump(['OUT', s, a])
            yield s

    luas = list(iter_luas(args))
    objs = list(iter_objs(luas))

    unit.onarchive_by_keys(['DONTCOMPRESS', 'NAME', 'LuaScripts.inc', 'KEYS', ':'.join(luas)] + objs)
    unit.onarchive_by_keys(['DONTCOMPRESS', 'NAME', 'LuaSources.inc', 'KEYS', ':'.join(luas)] + luas)


@macro
def LJ_21_ARCHIVE(unit: Unit, *args: tuple[str, ...]):
    """
    @usage: LJ_21_ARCHIVE(NAME Name LuaFiles...) # deprecated
    Precompile .lua files using LuaJIT 2.1 and archive both sources and results using sources names as keys
    """

    def iter_luas(data):
        for a in data:
            if a.endswith('.lua'):
                yield a

    def iter_objs(data):
        for a in data:
            s = a[:-3] + 'raw'
            unit.on_luajit_21_objdump(['OUT', s, a])
            yield s

    luas = list(iter_luas(args))
    objs = list(iter_objs(luas))

    unit.onarchive_by_keys(['DONTCOMPRESS', 'NAME', 'LuaScripts.inc', 'KEYS', ':'.join(luas)] + objs)
    unit.onarchive_by_keys(['DONTCOMPRESS', 'NAME', 'LuaSources.inc', 'KEYS', ':'.join(luas)] + luas)

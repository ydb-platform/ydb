def onpy_extralibs(unit, *args):
    """
    @usage: EXTRALIBS(liblist)
    Add external dynamic libraries during program linkage stage" }
    """

    libs = unit.get("OBJADDE_LIB_GLOBAL")
    changed = False
    if not libs:
        libs = ''
    for lib in args:
        if not lib.startswith('-'):
            lib = '-l' + lib
        if lib not in libs:
            libs = libs + ' ' + lib
            changed = True
    if changed:
        unit.set(["OBJADDE_LIB_GLOBAL", libs])

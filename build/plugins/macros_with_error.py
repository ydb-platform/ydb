import sys

import _common

import ymake


def onmacros_with_error(unit, *args):
    sys.stderr.write('This macros will fail\n')
    raise Exception('Expected fail in MACROS_WITH_ERROR')


def onrestrict_path(unit, *args):
    if args:
        if 'MSG' in args:
            pos = args.index('MSG')
            paths, msg = args[:pos], args[pos + 1 :]
            msg = ' '.join(msg)
        else:
            paths, msg = args, 'forbidden'
        if not _common.strip_roots(unit.path()).startswith(paths):
            error_msg = "Path '[[imp]]{}[[rst]]' is restricted - [[bad]]{}[[rst]]. Valid path prefixes are: [[unimp]]{}[[rst]]".format(
                unit.path(), msg, ', '.join(paths)
            )
            ymake.report_configure_error(error_msg)


def onassert(unit, *args):
    val = unit.get(args[0])
    if val and val.lower() == "no":
        msg = ' '.join(args[1:])
        ymake.report_configure_error(msg)


def onvalidate_in_dirs(unit, *args):
    files_var = args[0]
    pattern = args[1]
    no_srcdir = args[2] == '--'
    srcdir = '' if no_srcdir else args[2]
    dirs = args[3:] if no_srcdir else args[4:]
    pfx = '[[imp]]DECLARE_IN_DIRS[[rst]]: '

    if '**' in pattern:
        ymake.report_configure_error(f"{pfx}'**' in files mask is prohibited. Seen [[unimp]]{pattern}[[rst]]")
        unit.set([files_var, ""])

    for pat in ('*', '?'):
        if pat in srcdir:
            ymake.report_configure_error(
                f"{pfx}'{pat}' in [[imp]]SRCDIR[[rst]] argument is prohibited. Seen [[unimp]]{srcdir}[[rst]]"
            )
            unit.set([files_var, ""])

    for dir in dirs:
        for pat in ('*', '?', '$', '..'):
            if pat in dir:
                ymake.report_configure_error(
                    f"{pfx}'{pat}' in [[imp]]DIRS[[rst]] argument is prohibited. Seen [[unimp]]{dir}[[rst]]"
                )
                unit.set([files_var, ""])

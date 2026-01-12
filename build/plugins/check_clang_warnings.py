import ymake


def on_check_clang_warnings(unit, name, *args):
    for item in args:
        if item.startswith('-Wno-') or not item.startswith('-W'):
            ymake.report_configure_error(
                f'Warning tag [[alt1]]{item}[[rst]] is not allowed in call to macro [[alt1]]{name}[[rst]]'
            )

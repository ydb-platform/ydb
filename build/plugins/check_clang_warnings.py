import ymake


@ymake.macro
def _CHECK_CLANG_WARNINGS(unit: ymake.Unit, name: str, *args: tuple[str, ...]):
    for item in args:
        if item.startswith('-Wno-') or not item.startswith('-W'):
            ymake.report_configure_error(
                f'Warning tag [[alt1]]{item}[[rst]] is not allowed in call to macro [[alt1]]{name}[[rst]]'
            )

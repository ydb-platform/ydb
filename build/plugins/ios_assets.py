import ymake
import os


@ymake.macro
def IOS_ASSETS(unit: ymake.Unit, *, ROOT: str = '', CONTENTS: tuple[str, ...] = (), FLAGS: tuple[str, ...] = ()):
    if not ROOT and CONTENTS:
        ymake.report_configure_error('Please specify ROOT directory for assets')

    destination_root = os.path.normpath(os.path.join('$BINDIR', os.path.basename(ROOT)))
    rel_list = []
    for cont in CONTENTS:
        rel = os.path.relpath(cont, ROOT)
        if rel.startswith('..'):
            ymake.report_configure_error('{} is not subpath of {}'.format(cont, ROOT))
        rel_list.append(rel)
    if not rel_list:
        return
    results_list = [os.path.join('$B', unit.path()[3:], os.path.basename(ROOT), i) for i in rel_list]
    if len(CONTENTS) != len(results_list):
        ymake.report_configure_error('IOS_ASSETTS content length is not equals results')
    for s, d in zip(CONTENTS, results_list):
        unit.oncopy_file([s, d])
    if FLAGS:
        unit.onios_app_assets_flags(list(FLAGS))
    unit.on_ios_assets([destination_root] + results_list)

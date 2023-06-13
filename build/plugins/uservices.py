import json
import ymake


def on_process_usrv_files(unit, *args):
    mode = None
    if args[0] == 'NO_DEPS':
        for f in args[1:]:
            if f == 'OUT_NOAUTO':
                mode = f
                continue
            if mode is not None:
                unit.on_move([f + '.usrv', mode, f])
            elif f.endswith('.cpp'):
                unit.on_move([f + '.usrv', 'OUT', f])
            else:
                unit.on_move([f + '.usrv', 'OUT_NOAUTO', f])
        return

    deps_file = unit.resolve(unit.resolve_arc_path(args[0]))
    try:
        all_deps = json.load(open(deps_file, 'r'))
    except Exception as e:
        ymake.report_configure_error('Malformed dependencies JSON `{}`: {}'.format(args[0], e.__repr__()))
        return
    mode = 'OUT'
    for f in args[1:]:
        if f == 'OUT_NOAUTO':
            mode = f
            continue
        try:
            deps = all_deps[f]
        except KeyError:
            ymake.report_configure_error('Dependencies for {} not found in {}'.format(f, args[0]))
            unit.on_usrv_mv_with_deps([f])
            return
        unit.on_move([f + '.usrv', mode, f, 'CPP_DEPS'] + deps)

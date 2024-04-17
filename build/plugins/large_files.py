import json
import os
import ymake
from _common import resolve_common_const

PLACEHOLDER_EXT = "external"


def onlarge_files(unit, *args):
    """
    @usage LARGE_FILES([AUTOUPDATED] Files...)

    Use large file ether from working copy or from remote storage via placeholder <File>.external
    If <File> is present locally (and not a symlink!) it will be copied to build directory.
    Otherwise macro will try to locate <File>.external, parse it retrieve ot during build phase.
    """
    args = list(args)

    if args and args[0] == 'AUTOUPDATED':
        args = args[1:]

    for arg in args:
        if arg == 'AUTOUPDATED':
            unit.message(["warn", "Please set AUTOUPDATED argument before other file names"])
            continue

        src = unit.resolve_arc_path(arg)
        if src.startswith("$S"):
            msg = "Used local large file {}. Don't forget to run 'ya upload --update-external' and commit {}.{}".format(
                src, src, PLACEHOLDER_EXT
            )
            unit.message(["warn", msg])
            unit.oncopy_file([arg, arg])
        else:
            external = "{}.{}".format(arg, PLACEHOLDER_EXT)
            rel_placeholder = resolve_common_const(unit.resolve_arc_path(external))
            if not rel_placeholder.startswith("$S"):
                ymake.report_configure_error(
                    'LARGE_FILES: neither actual data nor placeholder is found for "{}"'.format(arg)
                )
                return
            try:
                abs_placeholder = unit.resolve(rel_placeholder)
                with open(abs_placeholder, "r") as f:
                    res_desc = json.load(f)
                    storage = res_desc["storage"]
                    res_id = res_desc["resource_id"]
            except Exception as e:
                ymake.report_configure_error(
                    'LARGE_FILES: error processing placeholder file "{}.": {}'.format(external, e)
                )
                return

            from_cmd = ['FILE', '{}'.format(res_id), 'OUT_NOAUTO', arg]
            if os.path.dirname(arg):
                from_cmd.extend(("RENAME", os.path.basename(arg)))

            method = getattr(unit, 'onfrom_{}'.format(storage.lower()), None)
            if method:
                method(from_cmd)
            else:
                ymake.report_configure_error(
                    'LARGE_FILES: error processing placeholder file "{}.": unknown storage kind "{}"'.format(
                        external, storage
                    )
                )

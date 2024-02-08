import json
import os
import six
from _common import rootrel_arc_src
import ymake


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def onresource_files(unit, *args):
    """
    @usage: RESOURCE_FILES([PREFIX {prefix}] [STRIP prefix_to_strip] {path})

    This macro expands into
    RESOURCE(DONT_PARSE {path} resfs/file/{prefix}{path}
        - resfs/src/resfs/file/{prefix}{remove_prefix(path, prefix_to_strip)}={rootrel_arc_src(path)}
    )

    resfs/src/{key} stores a source root (or build root) relative path of the
    source of the value of the {key} resource.

    resfs/file/{key} stores any value whose source was a file on a filesystem.
    resfs/src/resfs/file/{key} must store its path.

    DONT_PARSE disables parsing for source code files (determined by extension)
               Please don't abuse: use separate DONT_PARSE macro call only for files subject to parsing

    This form is for use from other plugins:
    RESOURCE_FILES([DEST {dest}] {path}) expands into RESOURCE({path} resfs/file/{dest})

    @see: https://wiki.yandex-team.ru/devtools/commandsandvars/resourcefiles/
    """
    prefix = ''
    prefix_to_strip = None
    dest = None
    res = []

    if args and not unit.enabled('_GO_MODULE'):
        # GO_RESOURCE currently doesn't support DONT_PARSE
        res.append('DONT_PARSE')

    args = iter(args)
    for arg in args:
        if arg == 'PREFIX':
            prefix, dest = next(args), None
        elif arg == 'DEST':
            dest, prefix = next(args), None
        elif arg == 'STRIP':
            prefix_to_strip = next(args)
        else:
            path = arg
            key = 'resfs/file/' + (
                dest or (prefix + (path if not prefix_to_strip else remove_prefix(path, prefix_to_strip)))
            )
            if key in res:
                unit.message(
                    ['warn', "Dublicated resource file {} in RESOURCE_FILES() macro. Skipped it.".format(path)]
                )
                continue
            src = 'resfs/src/{}={}'.format(key, rootrel_arc_src(path, unit))
            res += ['-', src, path, key]

    if unit.enabled('_GO_MODULE'):
        unit.on_go_resource(res)
    else:
        unit.onresource(res)


def on_all_resource_files(unit, macro, *args):
    # This is only validation, actual work is done in ymake.core.conf implementation
    for arg in args:
        if '*' in arg or '?' in arg:
            ymake.report_configure_error('Wildcards in [[imp]]{}[[rst]] are not allowed'.format(macro))


def onall_resource_files(unit, *args):
    on_all_resource_files(unit, 'ALL_RESOURCE_FILES', args)


def onall_resource_files_from_dirs(unit, *args):
    on_all_resource_files(unit, 'ALL_RESOURCE_FILES_FROM_DIRS', args)


def on_ya_conf_json(unit, conf_file):
    conf_abs_path = unit.resolve('$S/' + conf_file)
    if not os.path.exists(conf_abs_path):
        ymake.report_configure_error('File "{}" not found'.format(conf_abs_path))
        return

    # conf_file should be passed to the RESOURCE_FILES macro without path.
    # To resolve it later by name only we must add it's path to SRCDIR().
    conf_dir = os.path.dirname(conf_file)
    if conf_dir:
        unit.onsrcdir(conf_dir)
    unit.onresource_files(os.path.basename(conf_file))

    valid_dirs = (
        "build",
        conf_dir,
    )

    with open(conf_abs_path) as f:
        conf = json.load(f)
    formulas = set()
    for bottle_name, bottle in conf['bottles'].items():
        formula = bottle['formula']
        if isinstance(formula, six.string_types):
            if formula.startswith(valid_dirs):
                abs_path = unit.resolve('$S/' + formula)
                if os.path.exists(abs_path):
                    formulas.add(formula)
                else:
                    ymake.report_configure_error(
                        'File "{}" (referenced from bottle "{}" in "{}") is not found'.format(
                            abs_path, bottle_name, conf_abs_path
                        )
                    )
            else:
                ymake.report_configure_error(
                    'File "{}" (referenced from bottle "{}" in "{}") must be located in "{}" file tree'.format(
                        formula, bottle_name, conf_file, '" or "'.join(valid_dirs)
                    )
                )
    for formula in sorted(formulas):
        unit.onresource_files(formula)

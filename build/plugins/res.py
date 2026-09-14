import json
import os
import ymake

YA_CONF_JSON_NAME = "ya.conf.json"
DEFAULT_FORMULA_PATH_TMPL = "build/external_resources/{}/resources.json"


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


@ymake.macro
def RESOURCE_FILES(unit: ymake.Unit, *args: str):
    """
    @usage: RESOURCE_FILES([DONT_COMPRESS] [PREFIX {prefix}] [STRIP prefix_to_strip] {path})

    This macro expands into
    RESOURCE(DONT_PARSE {path} resfs/file/{prefix}{path}
        - resfs/src/resfs/file/{prefix}{remove_prefix(path, prefix_to_strip)}={rootrel_arc_src(path)}
    )

    resfs/src/{key} stores a source root (or build root) relative path of the
    source of the value of the {key} resource.

    resfs/file/{key} stores any value whose source was a file on a filesystem.
    resfs/src/resfs/file/{key} must store its path.

    DONT_COMPRESS allows optionally disable resource compression on platforms where it is supported

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

    if args and not unit.enabled('_GO_MODULE') and 'DONT_COMPRESS' in args:
        res.append('DONT_COMPRESS')

    args = iter(args)
    for arg in args:
        if arg == 'DONT_COMPRESS':
            pass
        elif arg == 'PREFIX':
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
                    ['warn', "Duplicated resource file {} in RESOURCE_FILES() macro. Skipped it.".format(path)]
                )
                continue
            src = 'resfs/src/{}=${{rootrel;context=TEXT;input=TEXT:"{}"}}'.format(key, path)
            res += ['-', src, path, key]

    if unit.enabled('_GO_MODULE'):
        unit.on_go_resource(res)
    else:
        unit.onresource(res)


@ymake.macro
def _ALL_RESOURCE_FILES(unit: ymake.Unit, macro: str, *args: str):
    # This is only validation, actual work is done in ymake.core.conf implementation
    for arg in args:
        if '*' in arg or '?' in arg:
            ymake.report_configure_error('Wildcards in [[imp]]{}[[rst]] are not allowed'.format(macro))


@ymake.macro
def ALL_RESOURCE_FILES(unit: ymake.Unit, *args: str):
    _ALL_RESOURCE_FILES(unit, 'ALL_RESOURCE_FILES', args)


@ymake.macro
def ALL_RESOURCE_FILES_FROM_DIRS(unit: ymake.Unit, *args: str):
    _ALL_RESOURCE_FILES(unit, 'ALL_RESOURCE_FILES_FROM_DIRS', args)


@ymake.macro
def _YA_TOOLS_CONF(unit: ymake.Unit, conf_dir: str):
    conf_dir = conf_dir.rstrip("/")
    conf_abs_path = unit.resolve('$S/' + conf_dir)
    if not os.path.isdir(conf_abs_path):
        ymake.report_configure_error('Directory "{}" not found'.format(conf_abs_path))
        return

    conf_file = conf_dir + "/" + YA_CONF_JSON_NAME
    conf_abs_file = conf_abs_path + "/" + YA_CONF_JSON_NAME
    if not os.path.isfile(conf_abs_file):
        ymake.report_configure_error('File "{}" not found'.format(conf_abs_file))
        return

    unit.onresource_files(["STRIP", conf_dir + "/", conf_file])

    resource_files = []
    formulas = set()
    valid_dirs = (
        "build",
        conf_dir,
    )

    def add_resource_file(abs_path):
        relative_path = remove_prefix(abs_path, conf_abs_path + "/")
        resource_files.append("/".join([conf_dir, relative_path]))

    def add_formula(formula, referenced_from):
        if not isinstance(formula, str):
            return

        if not any(formula.startswith(valid_dir + "/") for valid_dir in valid_dirs):
            ymake.report_configure_error(
                'File "{}" (referenced from {}) must be located in "{}" file tree'.format(
                    formula, referenced_from, '" or "'.join(valid_dirs)
                )
            )
            return

        formula_abs_path = unit.resolve('$S/' + formula)
        if os.path.exists(formula_abs_path):
            formulas.add(formula)
        else:
            ymake.report_configure_error(
                'File "{}" (referenced from {}) is not found'.format(formula_abs_path, referenced_from)
            )

    def add_bottle_formulas(config, config_file):
        for name, bottle in config["bottles"].items():
            add_formula(bottle["formula"], 'bottle "{}" in "{}"'.format(name, config_file))

    with open(conf_abs_file) as f:
        conf = json.load(f)

    if "simple_tools" in conf:
        for name, info in conf["simple_tools"].items():
            formula = DEFAULT_FORMULA_PATH_TMPL.format(info.get("resource", name))
            add_formula(formula, 'simple tool "{}" in "{}"'.format(name, conf_abs_file))
    add_bottle_formulas(conf, conf_abs_file)

    tools_dir = conf_abs_path + "/tools/tools"

    def add_tool_formula(tool_file):
        with open(tool_file) as f:
            tool = json.load(f).get("tool", {})
        if tool.get("type") != "simple":
            return

        definition = tool.get("definition", {})
        if "formula" in definition:
            formula = definition["formula"]
        else:
            tool_name = remove_prefix(tool_file, tools_dir + "/")[: -len(".tool.json")]
            formula = DEFAULT_FORMULA_PATH_TMPL.format(tool_name)
        add_formula(formula, 'tool config "{}"'.format(tool_file))

    tier_file = conf_abs_path + "/tools/internal/tiers.json"
    if os.path.isfile(tier_file):
        add_resource_file(tier_file)

    if os.path.isdir(tools_dir):
        for root, dirs, files in os.walk(tools_dir):
            dirs.sort()
            for filename in sorted(files):
                if filename.endswith(".tool.json"):
                    tool_file = root + "/" + filename
                    add_resource_file(tool_file)
                    add_tool_formula(tool_file)

    toolchains_dir = conf_abs_path + "/tools/toolchains"
    if os.path.isdir(toolchains_dir):
        for filename in sorted(os.listdir(toolchains_dir)):
            abs_path = toolchains_dir + "/" + filename
            if filename.endswith(".toolchain.json") and os.path.isfile(abs_path):
                add_resource_file(abs_path)
                with open(abs_path) as f:
                    add_bottle_formulas(json.load(f), abs_path)

    if resource_files:
        unit.onresource_files(["PREFIX", "yatools", "STRIP", conf_dir] + sorted(resource_files))
    for formula in sorted(formulas):
        unit.onresource_files(formula)

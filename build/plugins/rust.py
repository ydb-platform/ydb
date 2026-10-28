"""Configure whole-crate Rust compilation and host build-script tools."""

import ymake


def values(unit, name):
    return [value for value in (unit.get(name) or "").split() if value != "$" + name]


@ymake.macro
def _RUST_CONFIGURE(unit: ymake.Unit):
    mode = unit.get("_RUST_MODE")
    if unit.get("_RUST_KIND") == "rlib" and mode == "target" and unit.get("SANITIZER_TYPE") == "memory":
        unit.onpeerdir(["contrib/restricted/rust_msan"])
    if unit.get("_RUST_KIND") == "rlib":
        suffix = "-host.rlib" if mode == "host" else ".rlib"
        unit.set(["_RUST_METADATA_OUTPUT", "${BINDIR}/${MODULE_PREFIX}${REALPRJNAME}" + suffix])
        unit.set(["_RUST_METADATA_OUTPUT_FLAG", "--metadata-output"])
        unit.set(["_RUST_BUILD_INFO_OUTPUT", "${BINDIR}/${MODULE_PREFIX}${REALPRJNAME}" + suffix[:-5] + ".build.tar"])
        unit.set(["_RUST_BUILD_INFO_OUTPUT_FLAG", "--build-info-output"])

    script = unit.get("_RUST_BUILD_SCRIPT")
    if script:
        unit.set(["_RUST_BUILD_SCRIPT_TOOL", '${tool:"' + script + '"}'])
        unit.set(["_RUST_BUILD_SCRIPT_FLAG", "--build-script-program"])

    macros = values(unit, "_RUST_PROC_MACROS")
    unit.set(["_RUST_PROC_TOOLS", " ".join('${tool:"' + path + '"}' for path in macros)])

    tools = []
    for value in values(unit, "_RUST_BUILD_TOOLS"):
        name, path = value.split("=", 1)
        tools.append(name + '=${tool:"' + path + '"}')
    unit.set(["_RUST_BUILD_TOOL_ENV", " ".join(tools)])

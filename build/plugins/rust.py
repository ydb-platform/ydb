"""Configure whole-crate Rust compilation and host build-script tools."""

import posixpath

import ymake


def values(unit, name):
    return [value for value in (unit.get(name) or "").split() if value != "$" + name]


@ymake.macro
def _RUST_CONFIGURE(unit: ymake.Unit):
    mode = unit.get("_RUST_MODE")
    if unit.get("_RUST_KIND") == "rlib":
        suffix = "-host.rlib" if mode == "host" else ".rlib"
        unit.set(["_RUST_METADATA_OUTPUT", "${BINDIR}/${MODULE_PREFIX}${REALPRJNAME}" + suffix])
        unit.set(["_RUST_METADATA_OUTPUT_FLAG", "--metadata-output"])

    script = unit.get("_RUST_BUILD_SCRIPT")
    if script:
        output = "rust-build-script-" + mode + ".tar"
        inputs = values(unit, "_RUST_SOURCES") + values(unit, "_RUST_INPUTS")
        unit.on_rust_run_build_script([script, mode, output] + inputs)
        unit.set(["_RUST_BUILD_OUTPUT", posixpath.join("${BINDIR}", output)])
        unit.set(["_RUST_BUILD_OUTPUT_FLAG", "--build-script"])

    macros = values(unit, "_RUST_PROC_MACROS")
    unit.set(["_RUST_PROC_TOOLS", " ".join('${tool:"' + path + '"}' for path in macros)])

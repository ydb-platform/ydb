from _common import rootrel_arc_src, skip_build_root
from ymake import macro, Unit


@macro
def LLVM_BC(
    unit: Unit,
    *args: tuple[str, ...],
    SYMBOLS: tuple[str, ...] = (),
    NAME: str = '',
    SUFFIX: str = '',
    GENERATE_MACHINE_CODE: bool = False,
    NO_COMPILE: bool = False
):
    obj_suf = SUFFIX if SUFFIX else '' + unit.get('OBJ_SUF')
    merged_bc = NAME + '_merged' + obj_suf + '.bc'
    out_bc = NAME + '_optimized' + obj_suf + '.bc'
    bcs = []
    for x in args:
        rel_path = rootrel_arc_src(x, unit)
        bc_path = '${ARCADIA_BUILD_ROOT}/' + skip_build_root(rel_path) + obj_suf + '.bc'
        if not NO_COMPILE:
            if x.endswith('.c'):
                llvm_compile = unit.onllvm_compile_c
            elif x.endswith('.ll'):
                llvm_compile = unit.onllvm_compile_ll
            else:
                llvm_compile = unit.onllvm_compile_cxx
            llvm_compile([rel_path, bc_path])
        bcs.append(bc_path)
    unit.onllvm_link([merged_bc] + bcs)
    passes = ['default<O2>', 'globalopt', 'globaldce']
    opt_opts = []
    if SYMBOLS:
        passes += ['internalize']
        # XXX: '#' used instead of ',' to overcome ymake tendency to split everything by comma
        opt_opts += ['-internalize-public-api-list=' + '#'.join(list(SYMBOLS))]
    # Add additional quotes for cmake build.
    # Generated final option for cmake looks like: -passes="..."
    opt_opts += ['\'-passes="{}"\''.format('${__COMMA__}'.join(passes))]
    unit.onllvm_opt([merged_bc, out_bc] + opt_opts)
    if GENERATE_MACHINE_CODE:
        unit.onllvm_llc([out_bc, '-O2'])
    else:
        unit.onresource([out_bc, '/llvm_bc/' + NAME])

import os.path as P
import shutil
import re
from os import unlink

from devtools.yamaker.fileutil import files, subcopy
from devtools.yamaker.modules import (
    GLOBAL,
    Library,
    Linkable,
    Recursable,
    Switch,
    Words,
)
from devtools.yamaker.project import CMakeNinjaNixProject


def llvm_post_build(self):
    def if_gcc(seq):
        return ["#ifdef __GNUC__"] + seq + ["#endif", ""]

    header = ["#pragma once", ""] + if_gcc(
        [
            "#pragma GCC diagnostic push",
            '#pragma GCC diagnostic ignored "-Wunused-parameter"',
        ]
    )
    footer = if_gcc(
        [
            "#pragma GCC diagnostic pop",
        ]
    )
    subcopy(self.srcdir, self.dstdir, ["include/"])
    for path in files(P.join(self.dstdir, "include/")):
        if path.endswith("/CMakeLists.txt"):
            unlink(path)
        if path.endswith(".h"):
            with open(path, "r+") as f:
                content = f.read()
                if content.startswith("\ufeff"):
                    content = content[1:]
                f.seek(0)
                f.write("\n".join(header + [content] + footer))


def llvm_post_install(self):
    with open(P.join(self.dstdir, "provides.pbtxt"), "a") as f:
        f.write('p { i: "llvm/include" addincl: "%s/include" addinclbuild: true }\n' % self.arcdir)

    if self.version.startswith("16"):
        shutil.copyfile(
            P.join(self.srcdir, "build", "include", "llvm", "Frontend", "OpenMP", "OMP.inc"),
            P.join(self.dstdir, "include", "llvm", "Frontend", "OpenMP", "OMP.inc"),
        )

    with self.yamakes["include"] as m:
        m.PROVIDES = ["llvm"]
        for rp in m.RUN_PROGRAM:
            args = rp.record.Cmd.Args
            if args[1] == "-gen-instr-info" and args[3].endswith("/X86"):
                # MSVC fails to compile X86InstrNameData in X86GenInstrInfo.inc due to
                # fatal error C1091: compiler limit: string exceeds 65535 bytes in length
                args.append("--long-string-literals=0")
    self.yamakes["lib/Target/X86"].NO_CLANG_COVERAGE = True
    with self.yamakes["lib/CodeGen/AsmPrinter"] as m:
        # DEVTOOLSSUPPORT-4214: ubsan needs typeinfo for ARMTargetStreamer used in ARMException.
        m.after(
            "PEERDIR",
            Switch(
                {
                    'SANITIZER_TYPE == "undefined"': Linkable(PEERDIR=[self.arcdir + "/lib/Target/ARM/MCTargetDesc"]),
                }
            ),
        )
    with self.yamakes["tools/gold"] as m:
        m.EXPORTS_SCRIPT = ["gold.exports"]
        m.PEERDIR.add("build/platform/binutils")
        m.CFLAGS.append("-I$BINUTILS_ROOT_RESOURCE_GLOBAL/include")  # plugin-api.h
    # Make Linux-only dependencies conditional.
    with self.yamakes["tools/lli"] as m:
        peerdir_linux = {self.arcdir + "/lib/ExecutionEngine/PerfJITEvents"}
        m.PEERDIR -= peerdir_linux
        m.after("PEERDIR", Switch(OS_LINUX=Linkable(PEERDIR=peerdir_linux)))

        # Add AArch64 dependencies
        target_subdirs = [
            "AArch64",
            "AArch64/AsmParser",
            "AArch64/TargetInfo",
            "AArch64/MCTargetDesc",
        ]
        peerdir_aarch64 = {self.arcdir + f"/lib/Target/{subdir}" for subdir in target_subdirs}
        m.after("PEERDIR", Switch(ARCH_AARCH64=Linkable(PEERDIR=peerdir_aarch64)))
    with self.yamakes["lib/Support"] as m:
        srcs_unix = [
            "BLAKE3/blake3_avx2_x86-64_unix.S",
            "BLAKE3/blake3_avx512_x86-64_unix.S",
            "BLAKE3/blake3_sse2_x86-64_unix.S",
            "BLAKE3/blake3_sse41_x86-64_unix.S",
        ]
        srcs_windows = [
            "BLAKE3/blake3_avx2_x86-64_windows_gnu.S",
            "BLAKE3/blake3_avx512_x86-64_windows_gnu.S",
            "BLAKE3/blake3_sse2_x86-64_windows_gnu.S",
            "BLAKE3/blake3_sse41_x86-64_windows_gnu.S",
        ]
        m.SRCS -= set(srcs_unix)
        m.after(
            "SRCS",
            Switch(
                OS_LINUX=Linkable(SRCS=srcs_unix),
                OS_DARWIN=Linkable(SRCS=srcs_unix),
                OS_WINDOWS=Linkable(SRCS=srcs_windows, LDFLAGS=['ntdll.lib']),
            ),
        )
    # Fix MacOS arm64 linking
    with self.yamakes["tools/dsymutil"] as dsymutil:
        dsymutil.after(
            "SRCS",
            Switch({"OS_DARWIN": Linkable(LDFLAGS=[Words("-framework", "CoreFoundation")])}),
        )
    # Provide convenience ADDINCL GLOBAL, adjust platform-dependent recurses.
    incs = [
        "${ARCADIA_BUILD_ROOT}/" + self.arcdir + "/include",
        self.arcdir + "/include",
    ]
    self.yamakes.peerdir_for_addincls(self.arcdir, incs)
    with self.yamakes["."] as m:
        recurse_linux = {"lib/ExecutionEngine/PerfJITEvents", "tools/gold"}
        self.yamakes["."] = self.module(
            Library,
            ADDINCL=list(map(GLOBAL, incs)),
            RECURSE=m.RECURSE - recurse_linux,
            after=dict(RECURSE=[Switch(OS_LINUX=Recursable(RECURSE=recurse_linux))]),
        )
    self.yamakes["lib/Support"].PEERDIR.add("library/cpp/sanitizer/include")
    self.yamakes["utils/TableGen"].PEERDIR.add("library/cpp/sanitizer/include")
    self.yamakes["lib/Analysis"].CFLAGS = []


llvm = CMakeNinjaNixProject(
    nixattr="llvmPackages_20.llvm",
    arcdir="contrib/libs/llvm20",
    write_provides=True,
    run_program_dir="include",
    copy_sources=[
        "lib/Support/BLAKE3/",
        "lib/Support/Unix/Mutex.inc",
        "lib/Support/Unix/RWMutex.inc",
        "lib/Support/Windows/",
        "lib/TargetParser/Windows/",
        "utils/gdb-scripts/",
    ],
    platform_dispatchers=[
        "include/llvm/Config/config.h",
        "include/llvm/Config/llvm-config.h",
    ],
    addincl_global={"include": [".~", "."]},
    disable_includes=[
        "curl/curl.h",
        "cygwin/version.h",
        "google/protobuf/struct.pb.h",
        "google/protobuf/text_format.h",
        "isl_int_gmp.h",
        "isl_int_imath.h",
        "libxml/xmlreader.h",
        "linux/smb.h",
        "lwp.h",
        "machine/elf.h",
        "os/signpost.h",
        "perfmon/",
        "sys/ps.h",
        "xar/",
        "z3.h",
        "InlinerSizeModel.h",
        "RegallocEvictModel.h",
        "tensorflow/",
        "llvm/DebugInfo/PDB/DIA/DIASession.h",
        "zstd.h",  # LLVM_ENABLE_ZSTD
        "httplib.h",  # LLVM_ENABLE_HTTPLIB
        "kstat.h",  # __sun__ && __svr4__
        "_Ccsid.h",  # __MVS__
        "sys/rseq.h",  # __GLIBC__ && HAVE_BUILTIN_THREAD_POINTER
        "RegAllocEvictModel.h",  # LLVM_HAVE_TF_AOT_REGALLOCEVICTMODEL
        "IntelJITEventsWrapper.h",
        "ittnotify.h",
        "__le_cwi.h",
        "OS.h",
        "ptrauth.h",
    ],
    build_targets=["tools/all"],
    filter_targets=re.compile(r"^libLLVM(?!Testing).*\.so$").search,
    install_targets=[
        # libraries
        "LLVMPerfJITEvents",  # need for tools/lli
        "LLVMDiff",  # need for tool llvm-diff
        "LLVMAnalysis",
        "LLVMAsmParser",
        "LLVMAsmPrinter",
        "LLVMGlobalISel",
        "LLVMSelectionDAG",
        "LLVMBinaryFormat",
        "LLVMBitReader",
        "LLVMBitWriter",
        "LLVMBitstreamReader",
        "LLVMCGData",
        "LLVMCodeGen",
        "LLVMDWARFLinker",
        "LLVMDWARFLinkerClassic",
        "LLVMDWARFLinkerParallel",
        "LLVMDWP",
        "LLVMDebugInfoCodeView",
        "LLVMDebugInfoDWARF",
        "LLVMDebugInfoGSYM",
        "LLVMDebugInfoMSF",
        "LLVMDebugInfoPDB",
        "LLVMDebugInfoBTF",
        "LLVMSymbolize",
        "LLVMDebuginfod",  # appeared in llvm14, need for tools/llvm-symbolizer
        "LLVMDemangle",
        "LLVMExecutionEngine",
        "LLVMFrontendAtomic",
        "LLVMFuzzerCLI",
        "LLVMInterpreter",  # lib/ExecutionEngine
        "LLVMJITLink",  # lib/ExecutionEngine
        "LLVMMCJIT",  # lib/ExecutionEngine
        "LLVMOptDriver",
        "LLVMOrcJIT",  # lib/ExecutionEngine
        "LLVMOrcShared",  # lib/ExecutionEngine
        "LLVMOrcDebugging",
        "LLVMOrcTargetProcess",  # lib/ExecutionEngine
        "LLVMRuntimeDyld",  # lib/ExecutionEngine
        "LLVMExtensions",
        "LLVMFileCheck",
        "LLVMFrontendOpenACC",
        "LLVMFrontendOpenMP",
        "LLVMFrontendHLSL",
        "LLVMFuzzMutate",
        "LLVMFrontendOffloading",
        "LLVMCore",
        "LLVMCodeGenTypes",
        "LLVMIRPrinter",
        "LLVMIRReader",
        "LLVMMIRParser",
        "LLVMInterfaceStub",
        "LLVMLTO",
        "LLVMLineEditor",
        "LLVMLinker",
        "LLVMMC",
        "LLVMMCA",
        "LLVMMCDisassembler",
        "LLVMMCParser",
        "LLVMObjCopy",
        "LLVMObject",
        "LLVMObjectYAML",
        "LLVMOption",
        "LLVMPasses",
        "LLVMProfileData",
        "LLVMCoverage",
        "LLVMRemarks",
        "LLVMSandboxIR",
        "LLVMSupport",
        "LLVMTableGen",
        "LLVMTableGenBasic",
        "LLVMTableGenCommon",
        "LLVMTarget",
        "LLVMTargetParser",
        "LLVMTelemetry",
        "LLVMTextAPIBinaryReader",
        "LLVMAArch64AsmParser",
        "LLVMAArch64CodeGen",
        "LLVMAArch64Desc",
        "LLVMAArch64Disassembler",
        "LLVMAArch64Info",
        "LLVMAArch64Utils",
        "LLVMARMAsmParser",
        "LLVMARMCodeGen",
        "LLVMARMDesc",
        "LLVMARMDisassembler",
        "LLVMARMInfo",
        "LLVMARMUtils",
        "LLVMBPFAsmParser",
        "LLVMBPFCodeGen",
        "LLVMBPFDesc",
        "LLVMBPFDisassembler",
        "LLVMBPFInfo",
        "LLVMNVPTXCodeGen",
        "LLVMNVPTXDesc",
        "LLVMNVPTXInfo",
        "LLVMPowerPCAsmParser",
        "LLVMPowerPCCodeGen",
        "LLVMPowerPCDesc",
        "LLVMPowerPCDisassembler",
        "LLVMPowerPCInfo",
        "LLVMWebAssemblyAsmParser",
        "LLVMWebAssemblyCodeGen",
        "LLVMWebAssemblyDesc",
        "LLVMWebAssemblyDisassembler",
        "LLVMWebAssemblyInfo",
        "LLVMWebAssemblyUtils",
        "LLVMX86AsmParser",
        "LLVMX86CodeGen",
        "LLVMX86Desc",
        "LLVMX86Disassembler",
        "LLVMX86TargetMCA",  # appeared in llvm14, need for tools/llvm-mca
        "LLVMX86Info",
        "LLVMLoongArchCodeGen",
        "LLVMLoongArchAsmParser",
        "LLVMLoongArchDisassembler",
        "LLVMLoongArchDesc",
        "LLVMLoongArchInfo",
        "LLVMTextAPI",  # appeared in llvm14, need for tools/llvm-profdata
        "LLVMDlltoolDriver",
        "LLVMLibDriver",
        "LLVMAggressiveInstCombine",
        "LLVMCFGuard",
        "LLVMCoroutines",
        "LLVMipo",
        "LLVMInstCombine",
        "LLVMInstrumentation",
        "LLVMObjCARCOpts",
        "LLVMScalarOpts",
        "LLVMTransformUtils",
        "LLVMTransform",
        "LLVMHipStdPar",
        "LLVMVectorize",
        "LLVMXRay",
        "LLVMWindowsDriver",
        "LLVMWindowsManifest",
        # tools
        "bugpoint",
        "dsymutil",
        "llvm-tblgen",
        "llc",
        "lli",
        "lli-child-target",
        "llvm-ar",
        "llvm-as",
        "llvm-bcanalyzer",
        "llvm-cat",
        "llvm-cfi-verify",
        "LLVMCFIVerify",
        "llvm-config",
        "llvm-cov",
        "llvm-cvtres",
        "llvm-cxxdump",
        "llvm-cxxfilt",
        "llvm-cxxmap",
        "llvm-diff",
        "llvm-dis",
        "llvm-dwarfdump",
        "llvm-dwp",
        "llvm-exegesis",
        "LLVMExegesis",
        "LLVMExegesisAArch64",
        "LLVMExegesisPowerPC",
        "LLVMExegesisX86",
        "llvm-extract",
        "llvm-gsymutil",
        "llvm-ifs",
        "llvm-jitlink",
        "llvm-jitlink-executor",
        "llvm-libtool-darwin",
        "llvm-link",
        "llvm-lipo",
        "llvm-lto",
        "llvm-lto2",
        "llvm-mc",
        "llvm-mca",
        "llvm-ml",
        "llvm-modextract",
        "llvm-mt",
        "llvm-nm",
        "llvm-objcopy",
        "llvm-objdump",
        "llvm-opt-report",
        "llvm-pdbutil",
        "llvm-profdata",
        "llvm-profgen",
        "llvm-rc",
        "llvm-readobj",
        "llvm-reduce",
        "llvm-rtdyld",
        "llvm-size",
        "llvm-split",
        "llvm-stress",
        "llvm-strings",
        "llvm-symbolizer",
        "llvm-undname",
        "llvm-xray",
        "LTO",
        "obj2yaml",
        "opt",
        "Remarks",
        "Polly",
        "PollyISL",
        "PollyPPCG",
        "remarks-shlib",
        "sancov",
        "sanstats",
        "split-file",
        "verify-uselistorder",
        "yaml2obj",
        # utils
        "LLVMTableGenGlobalISel",
        # linux only lib
        "LLVMgold",
    ],
    post_build=llvm_post_build,
    post_install=llvm_post_install,
    ignore_commands=["llvm-min-tblgen"],
)

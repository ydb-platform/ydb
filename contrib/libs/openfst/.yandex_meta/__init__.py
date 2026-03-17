from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.project import NixProject

INSTALL_TARGETS = [
    "fstcompact",
    "fstcompressscript",
    "fstconst",
    "fstfar",
    "fstfarscript",
    "fstlinearscript",
    "fstlookahead",
    "fstmpdtscript",
    "fstngram",
    "fstpdtscript",
    "fstscript",
]

GLOBAL_SRCS = {
    "arc_lookahead-fst.cc",
    "compact16_acceptor-fst.cc",
    "compact16_string-fst.cc",
    "compact16_unweighted_acceptor-fst.cc",
    "compact16_unweighted-fst.cc",
    "compact16_weighted_string-fst.cc",
    "compact64_acceptor-fst.cc",
    "compact64_string-fst.cc",
    "compact64_unweighted_acceptor-fst.cc",
    "compact64_unweighted-fst.cc",
    "compact64_weighted_string-fst.cc",
    "compact8_acceptor-fst.cc",
    "compact8_string-fst.cc",
    "compact8_unweighted_acceptor-fst.cc",
    "compact8_unweighted-fst.cc",
    "compact8_weighted_string-fst.cc",
    "const16-fst.cc",
    "const64-fst.cc",
    "const8-fst.cc",
    "ilabel_lookahead-fst.cc",
    "linear-classifier-fst.cc",
    "linear-tagger-fst.cc",
    "ngram-fst.cc",
    "olabel_lookahead-fst.cc",
    "src/lib/fst-types.cc",
}


def post_install(self):
    for module in self.yamakes.values():
        for src in GLOBAL_SRCS & module.SRCS:
            module.SRCS.remove(src)
            module.SRCS.add(ArcPath(src, GLOBAL=True))


openfst = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/openfst",
    nixattr="openfst",
    license="Apache-2.0",
    put_with={"fstscript": ["fst"]},
    addincl_global={".": {"./src/include"}},
    copy_sources=["src/include/fst/extensions/mpdt/read_write_utils.h"],
    post_install=post_install,
    install_targets=INSTALL_TARGETS,
)

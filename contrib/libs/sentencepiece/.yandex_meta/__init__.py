import re

from devtools.yamaker.arcpath import ArcPath
from devtools.yamaker.fileutil import re_sub_dir
from devtools.yamaker.project import CMakeNinjaNixProject


def post_build(self):
    re_sub_dir(self.dstdir, "third_party/absl", "y_absl")
    re_sub_dir(self.dstdir, r"\bstd::string\b", "TString")
    re_sub_dir(self.dstdir, re.escape("return os.str();"), "return TString(os.str());")
    re_sub_dir(self.dstdir, "absl::string_view", "std::string_view")
    re_sub_dir(self.dstdir, "absl::", "y_absl::")
    re_sub_dir(self.dstdir, r"\bABSL_", "Y_ABSL_")


def post_install(self):
    m = self.yamakes["."]
    # Use util for TString.
    m.NO_UTIL = False
    m.CFLAGS.remove("-D_USE_INTERNAL_STRING_VIEW")  # Patched out.
    # Provide generated .pb.h.
    m.ADDINCL.add(ArcPath(self.arcdir + "/src", build=True, GLOBAL=True))


sentencepiece = CMakeNinjaNixProject(
    owners=["g:zeliboba", "g:cpp-contrib"],
    arcdir="contrib/libs/sentencepiece",
    nixattr="sentencepiece",
    build_targets=["sentencepiece", "sentencepiece_train"],
    put={
        "sentencepiece": ".",
    },
    put_with={
        "sentencepiece": [
            "sentencepiece_train",
        ],
    },
    use_provides=[
        "contrib/restricted/abseil-cpp-tstring/.yandex_meta",
    ],
    disable_includes=[
        "third_party/protobuf-lite/",
        "unicode/",
    ],
    post_build=post_build,
    post_install=post_install,
)

from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as libre2:
        # TString support will be added by patches
        libre2.NO_UTIL = False

        libre2.before("SRCS", Switch(WITH_VALGRIND=Linkable(CFLAGS=[GLOBAL("-DRE2_ON_VALGRIND")])))

        libre2.PEERDIR.add("library/cpp/sanitizer/include")

    with self.yamakes["re2/testing"] as test:
        test.module = "GTEST"
        test.GTEST = ""
        # fmt: off
        test.PEERDIR = [
            peerdir
            for peerdir in test.PEERDIR
            if "contrib/restricted/googletest" not in peerdir
        ]
        # fmt: on
        fileutil.re_sub_dir(
            f"{self.dstdir}/re2/testing",
            "util/test.h",
            "library/cpp/testing/gtest/gtest.h",
        )
        test.EXPLICIT_DATA = True


re2 = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    nixattr="re2",
    arcdir="contrib/libs/re2",
    ignore_targets=[
        "exhaustive_test",
        "exhaustive1_test",
        "exhaustive2_test",
        "exhaustive3_test",
        "random_test",
        "regexp_benchmark",
    ],
    # merge all tests together and put ya.make into re2/testing
    # We will change test framework to GTEST during post_install
    put={
        "re2": ".",
        "testing": "re2/testing",
    },
    put_with={
        "testing": [
            "charclass_test",
            "compile_test",
            "dfa_test",
            "filtered_re2_test",
            "mimics_pcre_test",
            "parse_test",
            "possible_match_test",
            "re2_arg_test",
            "re2_test",
            "regexp_test",
            "required_prefix_test",
            "search_test",
            "set_test",
            "simplify_test",
            "string_generator_test",
        ],
    },
    use_provides=[
        "contrib/restricted/abseil-cpp/.yandex_meta",
    ],
    post_install=post_install,
    # Default re2 cmake build provides TARGET_INCLUDE_DIRECTORIES
    # which yamaker handles by default, exposing entire contrib/libs/re2 via ADDINCL GLOBAL.
    #
    # We do not want this to happen, hence we disable this option handling and inclink target headers into include/ directory instead,
    # which we then expose by the means of ADDINCL GLOBAL.
    addincl_global={
        ".": ["./include"],
    },
    inclink={
        "include/re2": [
            "re2/re2.h",
            "re2/stringpiece.h",
        ],
        "include/util": [
            "util/logging.h",
            "util/utf.h",
        ],
    },
    disable_includes=[
        # ifdef USEPCRE
        "pcre.h",
        # ifdef RE2_USE_ICU
        "unicode/",
    ],
    write_public_incs=False,
)

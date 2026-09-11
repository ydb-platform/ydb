# Every module here is a guest module built with the emscripten toolchain
# (BUILD_ONLY_IF(OS_EMSCRIPTEN) via webassembly_udf.inc), so recursing into
# them from a host build yields nothing but "will not be built" warnings.
IF (OS_EMSCRIPTEN)
    RECURSE(
        bridge_dict
        md5
        sdk
        text
        trie
        types
    )
ELSE()
    # Guest-side code that is plain C++ and worth testing on the host. The
    # RECURSE has to happen here rather than in trie/ya.make, because that
    # module and its RECURSEs are dropped outside an emscripten build.
    RECURSE_FOR_TESTS(
        trie/ut
    )
ENDIF()

# Every module here is a guest module built with the emscripten toolchain
# (BUILD_ONLY_IF(OS_EMSCRIPTEN) via webassembly_udf.inc), so recursing into
# them from a host build yields nothing but "will not be built" warnings.
IF (OS_EMSCRIPTEN)
    RECURSE(
        bridge_dict
        log_parsing
        md5
        sdk
        text
        trie
        types
    )
ELSE()
    # Guest-side code that is plain C++ and worth testing on the host. The
    # RECURSE has to happen here rather than in a guest module's ya.make,
    # because those modules and their RECURSEs are dropped on host builds.
    RECURSE_FOR_TESTS(
        log_parsing/ut
        trie/ut
    )
ENDIF()

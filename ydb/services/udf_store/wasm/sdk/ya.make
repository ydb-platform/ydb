DLL()

BUILD_ONLY_IF(OS_EMSCRIPTEN)

EXPORT_ALL_DYNAMIC_SYMBOLS()
LDFLAGS(-Wl,--allow-undefined)

NO_UTIL()
NO_LIBC()
NO_RUNTIME()
NO_PLATFORM()

WHOLE_ARCHIVE(
    contrib/restricted/emscripten/system/lib/c
    contrib/restricted/emscripten/system/lib/dlmalloc
    contrib/restricted/emscripten/system/lib/standalonewasm
    contrib/restricted/emscripten/system/lib/libc/musl/src/network
    # contrib/libs/cxxsupp/libcxx
    # contrib/libs/cxxsupp/libcxxabi
    # util
)

PEERDIR(
    contrib/restricted/emscripten/system/lib/c
    contrib/restricted/emscripten/system/lib/dlmalloc
    contrib/restricted/emscripten/system/lib/standalonewasm
    contrib/restricted/emscripten/system/lib/libc/musl/src/network
    # contrib/libs/cxxsupp/libcxx
    # contrib/libs/cxxsupp/libcxxabi
    # util
)

END()

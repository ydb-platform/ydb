DLL()

BUILD_ONLY_IF(OS_EMSCRIPTEN)

EXPORT_ALL_DYNAMIC_SYMBOLS()
LDFLAGS(-Wl,--allow-undefined)

NO_UTIL()
NO_LIBC()
NO_RUNTIME()
NO_PLATFORM()
STRIP()

WHOLE_ARCHIVE(
    contrib/libs/protobuf
    contrib/libs/zlib
    contrib/restricted/abseil-cpp-tstring
    contrib/restricted/google/utf8_range
)

PEERDIR(
    contrib/libs/protobuf
    contrib/libs/zlib
    contrib/restricted/abseil-cpp-tstring
    contrib/restricted/google/utf8_range
)

END()

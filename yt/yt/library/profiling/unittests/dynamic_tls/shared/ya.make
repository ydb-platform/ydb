DLL(profiling_dynamic_tls)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    profiling_dynamic_tls.cpp
)

PEERDIR(
    yt/yt/library/profiling/solomon
)

LDFLAGS(-Wl,-Bsymbolic)

END()

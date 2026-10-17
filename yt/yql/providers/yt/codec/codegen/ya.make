LIBRARY()

SRCS(
    yt_codec_cg.h  # gn: slot yt_codegen
)

PEERDIR()

END()

RECURSE(
    llvm16
    no_llvm
)

RECURSE_FOR_TESTS(ut)

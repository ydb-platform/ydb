LIBRARY()

SRCS(
    fake_llvm_symbolizer.cpp
)

PEERDIR(
    contrib/libs/llvm14/lib/DebugInfo/Symbolize
)

END()

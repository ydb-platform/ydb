LIBRARY()

SRCS(
    fake_llvm_symbolizer.cpp
)

PEERDIR(
    contrib/libs/llvm12/lib/DebugInfo/Symbolize
)

END()

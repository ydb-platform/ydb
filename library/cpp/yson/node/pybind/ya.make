PY23_NATIVE_LIBRARY() 

OWNER(
    inngonch
    g:yt
)

PEERDIR(
    library/cpp/pybind
    library/cpp/yson/node
)
SRCS(
    node.cpp
)

END()

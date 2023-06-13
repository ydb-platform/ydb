LIBRARY()

PEERDIR(
    contrib/restricted/abseil-cpp/absl/container
)

# WARN thegeorg@: removing this ADDINCL will break svn selective checkout. Just don't.
ADDINCL(
    contrib/restricted/abseil-cpp
)

SRCS(
    flat_hash_map.cpp
    flat_hash_set.cpp
)

END()

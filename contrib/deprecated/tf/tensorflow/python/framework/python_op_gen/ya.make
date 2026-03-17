LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(1.10.1)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/deprecated/tf/minimal
)

NO_COMPILER_WARNINGS()

SRCDIR(contrib/deprecated/tf/tensorflow/python/framework)

SRCS(
    python_op_gen.cc
    python_op_gen_internal.cc
)

END()

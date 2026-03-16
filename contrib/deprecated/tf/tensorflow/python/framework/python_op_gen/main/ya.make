LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(1.10.1)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/deprecated/tf/tensorflow/python/framework/python_op_gen
)

NO_COMPILER_WARNINGS()

SRCDIR(contrib/deprecated/tf/tensorflow/python/framework)

SRCS(
    GLOBAL python_op_gen_main.cc
)

END()

PROGRAM(gen_mpi_ops_py_wrappers_cc)

WITHOUT_LICENSE_TEXTS()

LDFLAGS(-Wl,-no-pie)

PEERDIR(
    contrib/deprecated/tf/tensorflow/python/framework/python_op_gen/main
    contrib/python/horovod/horovod/tensorflow
)

END()

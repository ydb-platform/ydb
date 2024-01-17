GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(legacy.pb.go)

END()

RECURSE(
    bug1052
    proto2_20160225_2fc053c5
    proto2_20160519_a4ab9ec5
    proto2_20180125_92554152
    proto2_20180430_b4deda09
    proto2_20180814_aa810b61
    proto2_20190205_c823c79e
    proto3_20160225_2fc053c5
    proto3_20160519_a4ab9ec5
    proto3_20180125_92554152
    proto3_20180430_b4deda09
    proto3_20180814_aa810b61
    proto3_20190205_c823c79e
)

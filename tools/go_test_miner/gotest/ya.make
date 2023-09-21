GO_TEST_FOR(tools/go_test_miner)

IF (GO_VET == "yes" OR GO_VET == "on")
    SET_APPEND(GO_VET_FLAGS -tests=false)
ENDIF()

END()


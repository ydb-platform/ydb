IF (OPENSOURCE AND OPENSOURCE_PROJECT == "ydb")
    MESSAGE(FATAL_ERROR "You are trying to use deprecated C++ SDK V2 in YDB repository, it is forbidden, please use SDK V3")
ENDIF()

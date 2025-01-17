LIBRARY()

SRCS(
    svnversion.cpp
    svn_interface.c
)

IF (OPENSOURCE_PROJECT == "yt-cpp-sdk")
    PEERDIR(build/scripts/c_templates)
ENDIF()

END()

RECURSE(
    test
)

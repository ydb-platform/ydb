LIBRARY()

SRCS(
    svnversion.cpp
    svn_interface.c
)

IF (OPENSOURCE_PROJECT == "yt-cpp-sdk")
    PEERDIR(build/scripts/c_templates)
ENDIF()

PEERDIR_FROM_POLICY(
    ALLOW
    contrib
)

END()

RECURSE(
    test
)

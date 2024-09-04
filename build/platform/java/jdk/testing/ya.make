PACKAGE()


# FIXME: make use of java/jdk/jdk8/jdk.json somehow
IF (OS_LINUX)
    SET(ID 1901306329)

ELSEIF (OS_DARWIN)
    SET(ID 1901326056)

ELSE ()
    SET(ID 1901510679)

ENDIF()

FROM_SANDBOX(FILE ${ID} OUT jdk.tar)

END()

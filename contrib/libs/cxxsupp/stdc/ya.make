LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(Service-proxy-version)

LICENSE(Service-Sourceless-Library)

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
)

NO_PLATFORM()
NO_RUNTIME()

IF (CXX_RT == "glibcxx_static")
ELSE()
    LDFLAGS(-lstdc++)
ENDIF()

END()

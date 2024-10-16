LIBRARY()

WITHOUT_LICENSE_TEXTS()

LICENSE(YandexOpen)

VERSION(2024-06-20)

SUBSCRIBER(
    g:contrib
    g:cpp-contrib
    pg
)

NO_PLATFORM()
NO_RUNTIME()

IF (CXX_RT == "glibcxx_static")
ELSE()
    LDFLAGS(-lstdc++)
ENDIF()

END()

LIBRARY()

WITHOUT_LICENSE_TEXTS()

FLATC_FLAGS(--scoped-enums)

SRCS(
    Schema.fbs
)

END()

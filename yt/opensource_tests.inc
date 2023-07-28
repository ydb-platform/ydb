IF (OPENSOURCE)
    IF (MODULE_KIND == PROTO_LIBRARY)
        EXCLUDE_TAGS(PY_PROTO)
    ENDIF()

    IF (MODULE_KIND == PY23_LIBRARY)
        EXCLUDE_TAGS(PY2)
    ENDIF()

    IF (MODULE_KIND == PY23_TEST)
        EXCLUDE_TAGS(PY2)
    ENDIF()

    RESTRICT_LICENSES(
        DENY REQUIRE_DISCLOSURE FORBIDDEN PROTESTWARE
        # https://st.yandex-team.ru/DTCC-553
        EXCEPT contrib/libs/linux-headers
        # CHYT
        EXCEPT contrib/libs/fmt
        # Java tests
        EXCEPT contrib/java/junit/junit/4.13
        EXCEPT contrib/java/javax/persistence/persistence-api/1.0
    )
ENDIF()

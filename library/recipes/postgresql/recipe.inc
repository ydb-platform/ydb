BUILD_ONLY_IF(OS_LINUX OS_DARWIN)
BUILD_ONLY_IF(WARNING ARCH_X86_64)


IF (OS_DARWIN)
    DATA(sbr://4535784741)  # postgres
ELSEIF (OS_LINUX)
    DATA(sbr://678806418)  # postgres
ENDIF()
DEPENDS(library/recipes/postgresql)

IF (NOT DEFINED PG_MAX_CONNECTIONS)
    SET(PG_MAX_CONNECTIONS 20)
ENDIF()

IF (DEFINED PG_MIGRATIONS_DIR)
    IF (DEFINED PG_SCHEMA_MIGRATIONS_DIR)
        USE_RECIPE(library/recipes/postgresql/postgresql_recipe -m ${PG_MIGRATIONS_DIR} -s ${PG_SCHEMA_MIGRATIONS_DIR} -n ${PG_MAX_CONNECTIONS})
    ELSE()
        USE_RECIPE(library/recipes/postgresql/postgresql_recipe -m ${PG_MIGRATIONS_DIR} -n ${PG_MAX_CONNECTIONS})
    ENDIF()
ELSE()
    IF (DEFINED PG_SCHEMA_MIGRATIONS_DIR)
        USE_RECIPE(library/recipes/postgresql/postgresql_recipe -s ${PG_SCHEMA_MIGRATIONS_DIR} -n ${PG_MAX_CONNECTIONS})
    ELSE()
        USE_RECIPE(library/recipes/postgresql/postgresql_recipe -n ${PG_MAX_CONNECTIONS})
    ENDIF()
ENDIF()

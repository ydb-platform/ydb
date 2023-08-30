LIBRARY()

NO_SANITIZE()

NO_RUNTIME()

IF (CLANG16)
ADDINCL(GLOBAL contrib/libs/clang16-rt/include)
ELSE()
ADDINCL(GLOBAL contrib/libs/clang14-rt/include)
ENDIF()

END()


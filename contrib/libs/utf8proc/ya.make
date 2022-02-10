LIBRARY() 
 
LICENSE(
    MIT AND
    Unicode
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

OWNER(
    g:contrib
    g:cpp-contrib
)
 
VERSION(2.1.1) 
 
NO_UTIL() 
 
NO_COMPILER_WARNINGS() 
 
CFLAGS(
    GLOBAL -DUTF8PROC_STATIC
)
 
IF (NOT OS_WINDOWS) 
    CFLAGS(
        -std=c99
    )
ENDIF() 
 
SRCS( 
    utf8proc.c 
) 
 
END() 

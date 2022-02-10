RECURSE_ROOT_RELATIVE( 
    library/cpp/tvmauth 
    library/cpp/tvmauth/client 
    library/go/yandex/tvm 
    library/go/yandex/tvm/tvmauth 
    library/java/ticket_parser2 
    library/java/tvmauth 
    library/python/deprecated/ticket_parser2 
    library/python/tvmauth 
) 
 
IF (NOT OS_WINDOWS) 
    RECURSE_ROOT_RELATIVE( 
        library/c/tvmauth/src/ut 
    ) 
    IF (NOT SANITIZER_TYPE) 
        RECURSE_ROOT_RELATIVE( 
            library/c/tvmauth/src/ut_export 
        ) 
    ENDIF() 
ENDIF() 

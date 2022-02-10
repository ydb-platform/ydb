OWNER( 
    galaxycrab 
    g:yq 
    g:yql 
) 
 
IF (NOT OPENSOURCE)
    RECURSE(
        interface
        lib
    )
ELSE()
    RECURSE(
        interface
    )
ENDIF()

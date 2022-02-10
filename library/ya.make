OWNER(g:cpp-contrib)

RECURSE(
    c
    cpp
    python
    recipes
    #lemmer/byk/ut
)

IF (NOT SANITIZER_TYPE) 
    RECURSE( 
        go 
        java
    ) 
ENDIF() 
 
NEED_CHECK()

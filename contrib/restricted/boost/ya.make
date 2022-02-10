LIBRARY()

LICENSE( 
    BSD-3-Clause AND 
    BSL-1.0 AND 
    MIT AND 
    Mit-Old-Style AND 
    NCSA AND 
    Public-Domain AND 
    Zlib 
) 

LICENSE_TEXTS(.yandex_meta/licenses.list.txt) 
 
VERSION(1.67)

OWNER( 
    antoshkka 
    g:cpp-committee 
    g:cpp-contrib 
) 

NO_UTIL()

# This is the header-only version of boost
# To use a boost library with separate compilation units, 
# one should use `contrib/restricted/boost/libs/$lib`

ADDINCL(
    GLOBAL contrib/restricted/boost
)

END()

RECURSE(
    libs
)

RECURSE_FOR_TESTS(
    arcadia_test
)

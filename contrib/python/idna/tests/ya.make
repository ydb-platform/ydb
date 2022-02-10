PY23_TEST() 
 
OWNER(g:python-contrib yaskevich) 
 
PEERDIR( 
    contrib/python/idna 
) 
 
ENV(LC_ALL=ru_RU.UTF-8) 
ENV(LANG=ru_RU.UTF-8) 
 
TEST_SRCS( 
    test_idna_compat.py 
    test_idna.py 
    test_intranges.py 
    test_idna_codec.py 
    test_idna_other.py 
    test_idna_uts46.py 
) 
 
NO_LINT() 
FORK_SUBTESTS() 
 
END() 

FUZZ()

OWNER( 
    g:solomon 
    msherbakov 
) 

PEERDIR(
    library/cpp/monlib/encode/json
    library/cpp/monlib/encode/fake
)

SIZE(MEDIUM)

SRCS( 
    main.cpp 
) 

END()

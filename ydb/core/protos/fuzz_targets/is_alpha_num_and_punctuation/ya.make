FUZZ()

SRCS(
    main.cpp
    local_alpha_num_and_punctuation.cpp
)

CFLAGS(-Wno-deprecated-declarations)

END()

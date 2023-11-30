PROGRAM()

PEERDIR(
    library/cpp/getopt/small
    library/cpp/deprecated/fgood
)

INDUCED_DEPS(h+cpp
    ${ARCADIA_ROOT}/library/cpp/fieldcalc/field_calc_int.h
)

SRCS(
    parsestruct.rl
)

END()

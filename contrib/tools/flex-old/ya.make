PROGRAM(flex)

VERSION(2.5.4)

NO_RUNTIME()
NO_OPTIMIZE()
NO_COMPILER_WARNINGS()

SRCS(
    ccl.c
    dfa.c
    ecs.c
    gen.c
    main.c
    misc.c
    nfa.c
    skel.c
    sym.c
    tblcmp.c
    yylex.c
    scan.c
    parse.c
)

INDUCED_DEPS(h+cpp
    ${ARCADIA_ROOT}/contrib/tools/flex-old/FlexLexer.h
)

END()

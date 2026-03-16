PY3_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(PSF-2.0)

NO_LINT()
NO_COMPILER_WARNINGS()

CFLAGS(
    -Dtok_dump=_typed_ast_ast3_tok_dump
    -Ddumptree=_typed_ast_ast3_dumptree
    -Dprinttree=_typed_ast_ast3_printtree
    -Dshowtree=_typed_ast_ast3_showtree
)

PY_REGISTER(typed_ast._ast3)

ADDINCL(contrib/python/typed-ast/ast3/Include)

SRCS(
    Custom/typed_ast.c
    Parser/acceler.c
    Parser/bitset.c
    Parser/grammar.c
    Parser/grammar1.c
    Parser/node.c
    Parser/parser.c
    Parser/parsetok.c
    Parser/tokenizer.c
    Python/asdl.c
    Python/ast.c
    Python/graminit.c
    Python/Python-ast.c
)

END()

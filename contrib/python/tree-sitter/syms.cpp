#include <library/python/symbols/registry/syms.h>
#include <contrib/libs/tree_sitter/core/lib/include/tree_sitter/api.h>

extern "C" {
    extern const TSLanguage *tree_sitter_c(void);
    extern const TSLanguage *tree_sitter_cpp(void);
    extern const TSLanguage *tree_sitter_c_sharp(void);
    extern const TSLanguage *tree_sitter_go(void);
    extern const TSLanguage *tree_sitter_java(void);
    extern const TSLanguage *tree_sitter_kotlin(void);
    extern const TSLanguage *tree_sitter_proto(void);
    extern const TSLanguage *tree_sitter_python(void);
    extern const TSLanguage *tree_sitter_rust(void);
    extern const TSLanguage *tree_sitter_scala(void);
    extern const TSLanguage *tree_sitter_sql(void);
    extern const TSLanguage *tree_sitter_swift(void);
    extern const TSLanguage *tree_sitter_typescript(void);
    extern const TSLanguage *tree_sitter_javascript(void);
    extern const TSLanguage *tree_sitter_ruby(void);
    extern const TSLanguage *tree_sitter_tsx(void);
    extern const TSLanguage *tree_sitter_css(void);
}

BEGIN_SYMS("grammars")

SYM(tree_sitter_c)
SYM(tree_sitter_cpp)
SYM(tree_sitter_go)
SYM(tree_sitter_java)
SYM(tree_sitter_kotlin)
SYM(tree_sitter_proto)
SYM(tree_sitter_python)
SYM(tree_sitter_rust)
SYM(tree_sitter_scala)
SYM(tree_sitter_sql)
SYM(tree_sitter_swift)
SYM(tree_sitter_typescript)
SYM(tree_sitter_javascript)
SYM(tree_sitter_c_sharp)
SYM(tree_sitter_ruby)
SYM(tree_sitter_tsx)
SYM(tree_sitter_css)

END_SYMS()

from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_typescript = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/tsx",
    nixattr="tree-sitter-grammars.tree-sitter-tsx",
    nixsrcdir="source",
    copy_sources=[
        "common/**/*.h",
        "tsx/src/**/*.c",
        "tsx/src/**/*.cc",
    ],
    post_install=tree_sitter.grammar_post_install,
)

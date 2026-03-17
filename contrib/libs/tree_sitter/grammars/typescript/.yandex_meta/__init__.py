from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_typescript = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/typescript",
    nixattr="tree-sitter-grammars.tree-sitter-typescript",
    nixsrcdir="source",
    copy_sources=[
        "common/**/*.h",
        "typescript/src/**/*.c",
        "typescript/src/**/*.cc",
    ],
    post_install=tree_sitter.grammar_post_install,
)

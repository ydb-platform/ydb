from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_go = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/go",
    nixattr="tree-sitter-grammars.tree-sitter-go",
    nixsrcdir="source",
    copy_sources=[
        "src/**/*.c",
        "src/**/*.cc",
    ],
    post_install=tree_sitter.grammar_post_install,
)

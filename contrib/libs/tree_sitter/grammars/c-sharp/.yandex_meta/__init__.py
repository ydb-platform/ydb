from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_c_sharp = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/c-sharp",
    nixattr="tree-sitter-grammars.tree-sitter-c-sharp",
    nixsrcdir="source",
    copy_sources=[
        "src/**/*.h",
        "src/**/*.c",
        "src/**/*.cc",
    ],
    post_install=tree_sitter.grammar_post_install,
)

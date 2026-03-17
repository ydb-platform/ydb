from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_scala = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/scala",
    nixattr="tree-sitter-grammars.tree-sitter-scala",
    nixsrcdir="source",
    copy_sources=["src/**/*.c", "src/**/*.cc", "src/*.h"],
    post_install=tree_sitter.grammar_post_install,
)

from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_sql = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/sql",
    nixattr="tree-sitter-grammars.tree-sitter-sql",
    nixsrcdir="source",
    copy_sources=["src/**/*.c", "src/**/*.cc", "src/*.h"],
    post_install=tree_sitter.grammar_post_install,
)

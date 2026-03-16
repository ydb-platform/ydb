from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_ruby = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/ruby",
    nixattr="tree-sitter-grammars.tree-sitter-ruby",
    nixsrcdir="source",
    copy_sources=[
        "src/**/*.h",
        "src/**/*.c",
        "src/**/*.cc",
    ],
    post_install=tree_sitter.grammar_post_install,
)

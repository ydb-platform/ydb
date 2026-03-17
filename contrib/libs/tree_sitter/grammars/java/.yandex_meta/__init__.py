from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_java = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/java",
    nixattr="tree-sitter-grammars.tree-sitter-java",
    nixsrcdir="source",
    copy_sources=[
        "src/**/*.c",
        "src/**/*.cc",
    ],
    post_install=tree_sitter.grammar_post_install,
)

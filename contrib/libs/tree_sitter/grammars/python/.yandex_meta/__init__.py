from devtools.yamaker import tree_sitter
from devtools.yamaker.project import NixSourceProject

tree_sitter_python = NixSourceProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/grammars/python",
    nixattr="tree-sitter-grammars.tree-sitter-python",
    nixsrcdir="source",
    copy_sources=[
        "src/**/*.c",
        "src/**/*.h",
        "src/**/*.cc",
    ],
    post_install=tree_sitter.grammar_post_install,
)

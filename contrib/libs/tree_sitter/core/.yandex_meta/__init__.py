from devtools.yamaker.project import GNUMakeNixProject


def post_install(self):
    with self.yamakes["."] as tree_sitter:
        tree_sitter.ADDINCL += [
            f"{self.arcdir}/lib/include",
            f"{self.arcdir}/lib/src",
        ]


tree_sitter = GNUMakeNixProject(
    owners=["g:codesearch", "g:cpp-contrib"],
    arcdir="contrib/libs/tree_sitter/core",
    nixattr="tree-sitter",
    nixsrcdir="source",
    post_install=post_install,
)

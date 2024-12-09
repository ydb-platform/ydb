from devtools.yamaker.project import NixProject


def post_install(self):
    m = self.yamakes["."]
    m.SRCS -= {
        "src/xxhash/xxhash.c",
    }
    m.PEERDIR += {"contrib/libs/xxhash"}


libfyaml = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libfyaml",
    nixattr="libfyaml",
    unbundle_from={
        "xxhash": "src/xxhash",
    },
    install_targets=[
        "fyaml",
    ],
    post_install=post_install,
)

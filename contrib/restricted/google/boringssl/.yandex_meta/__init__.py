import os
from devtools.yamaker.project import NixProject


def post_install(self):
    # set prefixes
    for ymake in self.yamakes.values():
        ymake.CFLAGS.insert(0, "-DBORINGSSL_PREFIX=BSSL")

    # set BoringSSL paths to avoid OpenSSL usage
    for folder, subfolders, files in os.walk(self.dstdir):
        for file in files:
            filePath = os.path.abspath(os.path.join(folder, file))
            with open(filePath, "r") as file:
                filedata = file.read()
            filedata = filedata.replace("<openssl/", "<contrib/restricted/google/boringssl/include/openssl/")
            filedata = filedata.replace(
                '"openssl/arm_arch.h"', "<contrib/restricted/google/boringssl/include/openssl/arm_arch.h>"
            )
            filedata = filedata.replace(
                "<boringssl_prefix_symbols.h>",
                "<contrib/restricted/google/boringssl/include/boringssl_prefix_symbols.h>",
            )
            filedata = filedata.replace(
                "<boringssl_prefix_symbols_asm.h>",
                "<contrib/restricted/google/boringssl/include/boringssl_prefix_symbols_asm.h>",
            )
            filedata = filedata.replace("OPENSSL_armcap_P", "BSSL_armcap_P")

            with open(filePath, "w") as file:
                file.write(filedata)


boringssl = NixProject(
    owners=["g:balancer"],
    nixattr="boringssl",
    ignore_commands=["bash", "sed", "cat", "perl", "go", "mkdir", "echo"],
    arcdir="contrib/restricted/google/boringssl",
    use_full_libnames=True,
    install_targets=[
        "libcrypto",
        "libdecrepit",
        "libssl",
    ],
    put={
        "libcrypto": ".",
        "libssl": "ssl",
        "libdecrepit": "decrepit",
    },
    disable_includes=[
        "machine/armreg.h",
        "zircon/features.h",
        "zircon/syscalls.h",
        "zircon/types.h",
    ],
    copy_sources=[
        "include/openssl/*",
        "crypto/*.h",
        "third_party/fiat/*.h",
        "include/boringssl_prefix_symbols*",
    ],
    post_install=post_install,
)

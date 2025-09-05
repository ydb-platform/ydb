{% note info %}

If you are using a Mac with an Apple Silicon processor, emulate the x86_64 CPU instruction set with [Rosetta](https://support.apple.com/en-us/102527):

- [colima](https://github.com/abiosoft/colima) with the `colima start --arch aarch64 --vm-type=vz --vz-rosetta` options.
- [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) with installed and enabled Rosetta 2.

If Rosetta 2 is not enabled, add the `-e YDB_USE_IN_MEMORY_PDISKS=true` parameter to the command for running the Docker container. For more information, see [{#T}](../configuration.md).

{% endnote %}

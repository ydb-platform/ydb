{% note info %}

If you are using a Mac with an Apple Silicon processor, emulate the x86_64 CPU instruction set with [Rosetta](https://support.apple.com/en-us/102527):

- [colima](https://github.com/abiosoft/colima) with the `colima start --arch aarch64 --vm-type=vz --vz-rosetta` options.
- [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) with installed and enabled Rosetta 2.

{% endnote %}

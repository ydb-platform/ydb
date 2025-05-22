{% note info %}

При использовании Mac с процессором Apple Silicon, набор процессорных инструкций x86_64 можно эмулировать с помощью [Rosetta](https://support.apple.com/en-us/102527):

- [colima](https://github.com/abiosoft/colima) c параметрами `colima start --arch aarch64 --vm-type=vz --vz-rosetta`;
- [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) с установленной и включённой Rosetta 2.

{% endnote %}

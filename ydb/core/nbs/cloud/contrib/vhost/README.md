<div style="display: flex; align-items: center;">
  <img style="margin-bottom: 5px;" src="docs/logo.svg" width="40px" align="left"/>
  <h1 style="margin-left: 5px; margin-bottom: 5px;">Libvhost</h1>
</div>

[![CI](https://github.com/yandex-cloud/yc-libvhost-server/actions/workflows/main.yaml/badge.svg)](https://github.com/yandex-cloud/yc-libvhost-server/actions/workflows/main.yaml)

A library for building [vhost-user protocol](https://qemu-project.gitlab.io/qemu/interop/vhost-user.html) servers.

## Quickstart

Building the project:
```bash
CC=clang meson setup build
ninja -C build
```

Running tests locally:
```
ninja test -C build
```

# Docker image

## Base image

Base image is official `ubuntu:focal` with installed packages:
- libcap2-bin (for setcap to binaries)
- ca-certificates (for working with CA bundle)

Also base image included `LICENSE`, `AUTHORS` and `README.md` files from root of repository
and dynamic cpp libraries `libiconv`, `liblibidn` and `libaio`.

### Base breakpad image

Extend base image with:
- additional packages to collect and manage minidump format (binutils, gdb, strace, linux-tools-generic)
- dynamic library `libbreakpad_init.so` from breakpad_init image (ydb/deploy/breakpad_init)
- environment variable `LD_PRELOAD` for loading breakpad library on process start
- environment variables `BREAKPAD_MINIDUMPS_PATH` and `BREAKPAD_MINIDUMPS_SCRIPT` to setup breakpad
- binaries `minidump_stackwalk` and `minidump-2-core` to collect stacktrace and convert in coredump format
- python script `minidump_script.py` as dumpCallback handler for breakpad

## Image Types

### Release

Image with minimal requirements to launch ydbd in container

```bash
ya package --docker ydb/deploy/docker/release/pkg.json
```

Used base image and included:
- ydb cli binary
- ydbd server strip'ed binary baked with build type `Release`

### Breakpad

Image with google breakpad assets to collect minidump

```bash
ya package --docker ydb/deploy/docker/breakpad/pkg.json
```

Used base breakpad image and included:

- ydb cli binary
- ydbd server binary baked with build flag `DEBUGINFO_LINES_ONLY`

### Debug

Image with debug symbols and utils for dev purposes

```bash
ya package --docker ydb/deploy/docker/debug/pkg.json
```

Used base breakpad image and included:
- additional packages with debug utils (dnsutils, telnet, netcat-openbsd, iputils-ping, curl)
- ydb cli binary
- ydbd server strip'ed binary baked with build type `Release`
- debug symbols for ydbd binary

# Docker image

## Base image

Base layer is official `ubuntu:20.04` docker image with packages:
- libcap2-bin (for setcap to binaries)
- ca-certificates (for working with CA bundle)

## Image Types

### Release

```bash
ya package --docker ydb/deploy/docker/pkg.json
```

Image with minimal requirements to launch ydbd in container.

The image includes:
- dynamic cpp libraries (libiconv, libidn, libaio)
- ydb cli binary
- ydbd server strip'ed binary


### Breakpad

```bash
ya package --docker ydb/deploy/docker/breakpad/pkg.json
```

Image with google breakpad assets to collect minidump instead of coredump.

Extend release image with:
- additional packages to collect and manage minidump format
- dynamic library `libbreakpad_init.so` from breakpad_init image (ydb/deploy/breakpad_init)
- environment variable `LD_PRELOAD` to load library on process start
- binaries `minidump_stackwalk` and `minidump-2-core` to collect stacktrace and convert to coredump format
- python script `minidump_script.py` as dumpCallback handler for google breakpad
- environment variables `BREAKPAD_MINIDUMPS_PATH` and `BREAKPAD_MINIDUMPS_SCRIPT` to setup breakpad

### Debug

```bash
ya package --docker ydb/deploy/docker/debug/pkg.json
```

Image with google breakpad assets to collect minidump instead of coredump.

Extend breakpad image with:
- additional packages with debug utils (dnsutils, telnet, netcat-openbsd, iputils-ping, curl)
- debug symbols for ydbd binary

## Additional Info

All types of images also included LICENSE and AUTHORS files from root of repository


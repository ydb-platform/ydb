# Building YDB Locally in Docker

## TL;DR: Ubuntu and Bash

You need a YDB checkout, Bash, and [Docker Engine](https://docs.docker.com/engine/install/ubuntu/).
The `docker info` command must work under your user account;
see the [Docker instructions](https://docs.docker.com/engine/install/linux-postinstall/) for access setup.
Run all commands below from the repository root in Bash.

```bash
set -o pipefail
ydb/tools/build_local/run.sh --check
ydb/tools/build_local/run.sh ydb/apps/ydbd \
    2>&1 | tee .ydb-build/build.log | tail -100
```

The script builds an Ubuntu 22.04 image and runs `ya make --build relwithdebinfo`.
By default, the container gets 4 CPUs and 24 GiB of RAM with no swap, and `ya` has a 20 GiB memory budget.
The compiler is downloaded by `ya`. The server binary will be available at
`.ydb-build/linux-amd64/ydb/apps/ydbd/ydbd`.

You can monitor build progress from another terminal:

```bash
tail -f .ydb-build/build.log
```

## Tests

Arguments are passed to `ya make`. Running tests also builds their dependencies:

```bash
set -o pipefail
ydb/tools/build_local/run.sh -tA ydb/core/kqp/ut/olap \
    -F 'KqpOlapLocks::*' 2>&1 | tail -100
```

To run a single C++ test, use `-F 'Suite::Test'`. To rerun tests that have already
passed, add `--retest`: the build cache is preserved.
See [TESTS.md](../../agents/TESTS.md) for more information about filters.

To run compatibility tests against the current source code, specify `current`:

```bash
set -o pipefail
ydb/tools/build_local/run.sh -tA ydb/tests/compatibility/olap \
    -DYDB_COMPAT_TARGET_REF=current \
    -F 'test_compression.py::*' 2>&1 | tail -100
```

Other versions are selected in
[versions.inc](../../tests/library/compatibility/versions.inc).
Without `current`, the tests may use only released versions of YDB.

## Resource Limits

Limits can be set for each run:

```bash
YDB_BUILD_CPUS=6 YDB_BUILD_MEMORY=32g YA_MEMORY_LIMIT=28G \
    ydb/tools/build_local/run.sh ydb/apps/ydbd
```

`YDB_BUILD_MEMORY` is the container's hard memory limit, and `YA_MEMORY_LIMIT` is
the build scheduler's memory budget. Leave headroom between them for the linker and tests.
Docker limits apply to the build container, not to the image build process.

| Variable | Default |
| --- | --- |
| `YDB_BUILD_CPUS` | `4` |
| `YDB_BUILD_MEMORY` | `24g` |
| `YA_MEMORY_LIMIT` | `20G` |
| `YDB_BUILD_TYPE` | `relwithdebinfo` |
| `YDB_BUILD_PLATFORM` | `linux/amd64` |
| `YDB_BUILD_OUTPUT` | `.ydb-build/<platform>` in the repository |
| `YDB_BUILD_CACHE` | `ydb-ya-cache-<platform>-<uid>-<gid>` |
| `YDB_BUILD_UID`, `YDB_BUILD_GID` | User's UID/GID on Linux, `0` on macOS |
| `DOCKER_CONTEXT` | Current Docker context |

On Linux, UID/GID allow build outputs to be written as the host user.
On macOS, root is used inside the VM to access directories mounted by Colima.

## Cache and Disk Space

Rerun the same command after making changes: `ya` reuses downloaded tools
and build outputs. Changing a `.cpp` file usually requires recompiling it and relinking,
while changing a shared header may affect many files. CPU and memory limits
do not affect cache keys. Do not modify source files during a build.

Source files are mounted read-only at `/src`, and the output directory is mounted at `/out`.
The `ya` cache is stored in a separate Docker volume and persists after the container stops.
To reuse it, use the same Docker context, platform, and UID/GID,
or set `YDB_BUILD_CACHE` explicitly. Image layers are also cached: YDB source files
are not copied into the image.

Building the server and several test binaries with debug information requires
hundreds of gigabytes. Allocate at least 300 GiB for Docker in the VM and additional
space on the host for build outputs; 150 GiB proved insufficient for this set of builds.
`run.sh --check` shows available disk space, inodes, and effective cgroup v2 limits.

To work inside the container:

```bash
ydb/tools/build_local/run.sh --shell
```

Inside the container, run `/usr/local/bin/ydb-build <target>` to keep the build settings.

## macOS / Apple Silicon

For Colima, run the following from the repository root:

```bash
colima start ydb-build --activate=false --cpu 4 --memory 26 --disk 300 \
    --vm-type vz --vz-rosetta --mount "$PWD:w" --ssh-config=false
export DOCKER_CONTEXT=colima-ydb-build
ydb/tools/build_local/run.sh --check
```

This creates a separate VM profile with memory headroom above the container limit.
`colima stop ydb-build` stops the VM while preserving the cache.
In Docker Desktop, configure VM resources in the settings.

By default, Apple Silicon builds Linux x86-64 binaries through Rosetta.
This is slower than a native build. `YDB_BUILD_PLATFORM=linux/arm64` selects a separate
image and cache, but not all targets have the required `ya` resources for ARM64.

If the Colima disk fills up, stop the build and expand the existing disk:

```bash
colima stop ydb-build
colima start ydb-build --activate=false --ssh-config=false --disk 400
ydb/tools/build_local/run.sh --check
```

Choose a size larger than the current one and check the available space on the host.
See the [Colima FAQ](https://colima.run/docs/faq/) for more information.

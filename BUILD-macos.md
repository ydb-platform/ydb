# Building YDB on an Apple Silicon Mac

This experimental recipe uses YDB's Clang 20 toolchain and macOS SDK 26.5. It pins a source revision and pending fixes checked on October 22, 2026. It is a development setup, not a supported production deployment configuration. Recheck the fix list before using a newer source revision.

The fixes have been used in local macOS builds and selected tests. Their application to the pinned revision has been checked. The SDK override below also resolved the `INFINITY` compilation failure on a second Apple Silicon Mac; a complete clean build and full test suite on that machine have not been verified.

Run the commands in order in the same terminal. Stop if a command fails.

## 1. Prepare the machine

You need a native ARM64 terminal, Git, Python 3, and an installed macOS 26.5 SDK from Xcode or Command Line Tools. Keep the checkout and build cache on an SSD with room for both sources and build artifacts; the general [build instructions](BUILD.md) require at least 80 GB of free space. Debug information and additional test targets can require substantially more.

```bash
uname -m
git --version
python3 --version
xcode-select -p
```

`uname -m` must print `arm64`. If Apple developer tools are missing, install them with `xcode-select --install`. A current installation may supply SDK 27 instead of 26.5. If necessary, obtain an Xcode or Command Line Tools release containing SDK 26.5 from [Apple Developer Downloads](https://developer.apple.com/download/all/) and install it before continuing.

Select the actual SDK directory. This example uses the Command Line Tools location; an Xcode installation may place it under `Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/` instead.

```bash
YDB_SDK=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk
test -r "$YDB_SDK/SDKSettings.json"
test -r "$YDB_SDK/usr/include/math.h"
python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["Version"])' \
  "$YDB_SDK/SDKSettings.json"
```

The version must be `26.5`. Use a path without spaces for the flags below. The presence of a newer Xcode does not require replacing it: the build explicitly selects the older SDK. `xcrun --sdk <absolute-path>` can fail to recognize an SDK outside the selected Xcode even when its files are present; the compiler check in step 4 verifies the selected headers directly.

## 2. Get the sources

Use a new checkout for this recipe. The remote is deliberately named `official`; the commands do not assume that an `origin` remote exists.

```bash
mkdir -p "$HOME/ydbwork"
cd "$HOME/ydbwork"
git clone --origin official https://github.com/ydb-platform/ydb.git ydb
cd ydb
git switch -c local/macos-build 1e590daa02c797f658ce1d606429ecfe86da236f
export YA_CACHE_DIR="$HOME/ydbwork/.ya"
```

This base is a `main` revision from October 22, 2026. Pinning it avoids applying an already-merged fix again when `main` advances.

## 3. Apply the fixes

Before cherry-picking, check your Git identity:

```bash
git config --get user.name
git config --get user.email
```

If either value is missing or incorrect, set `git config --local user.name` and `git config --local user.email` with your own name and email before continuing.

All PRs in this table were open when the recipe was checked. Fetch their public refs, then cherry-pick the exact commits below rather than moving PR heads.

| PR | Purpose |
| --- | --- |
| [#55856](https://github.com/ydb-platform/ydb/pull/55856) | Correct curl's `memrchr` configuration on macOS. |
| [#55847](https://github.com/ydb-platform/ydb/pull/55847) | Fix the ColumnShard snapshot integer type mismatch on macOS. |
| [#55768](https://github.com/ydb-platform/ydb/pull/55768) | Fix native MiniKQL JIT code generation on AArch64; includes two commits. |
| [#55893](https://github.com/ydb-platform/ydb/pull/55893) | Fix PostgreSQL thread cleanup on macOS. |
| [#56501](https://github.com/ydb-platform/ydb/pull/56501) | Exclude Darwin from the GNU Bison linker workaround; draft pending upstream integration. |
| [#56502](https://github.com/ydb-platform/ydb/pull/56502) | Work around the missing Darwin ARM64 cryptography Rust archive using the existing 3.3.2 fallback instead of 41.0.6; draft temporary workaround with different APIs and dependencies. |
| [#56503](https://github.com/ydb-platform/ydb/pull/56503) | Use loopback hostnames for dynamic nodes in local macOS test clusters. |

```bash
git fetch official \
  pull/55856/head:refs/remotes/official/pr-55856 \
  pull/55847/head:refs/remotes/official/pr-55847 \
  pull/55768/head:refs/remotes/official/pr-55768 \
  pull/55893/head:refs/remotes/official/pr-55893 \
  pull/56501/head:refs/remotes/official/pr-56501 \
  pull/56502/head:refs/remotes/official/pr-56502 \
  pull/56503/head:refs/remotes/official/pr-56503

git cherry-pick \
  f9b173456a22932c2da98a96f71d6805a9b52af4 \
  aebbc75762b294888694259dfee1f7db84ed5246 \
  67619c2e6b6951a965bbcf25b681c9608df36e30 \
  f52f2555703eb120a551d6e9055b6f39072f7290 \
  7596224d366a404202582a89fb46bcaa4dc80375 \
  230b67f0c63a0d1d77e64804adcaad06bbf1710d \
  8f5b996561cfc1823b4df4978d9f16f002c1d59e \
  0d4e6a046221a5b8797b2f458d26b41f90dac47c
```

If a PR has been rewritten and an exact commit is unavailable, recheck this recipe against that PR. Do not silently substitute its new head. On a conflict, stop and inspect the base and existing changes; `git cherry-pick --abort` cancels the current sequence.

## 4. Check the compiler and SDK

`ya` downloads its compiler. There is no need to switch to a Homebrew compiler or Clang 22. Select Clang 20 explicitly for this small diagnostic and check that its reported version is 20.x:

```bash
YDB_CXX="$(./ya tool --toolchain=clang20 --host-platform=darwin-arm64 --print-path c++)"
"$YDB_CXX" --version
printf '#include <random>\n#ifndef INFINITY\n#error INFINITY_is_missing\n#endif\n' |
  "$YDB_CXX" \
    --target=arm64-apple-macos11 -mmacosx-version-min=11.0 \
    --sysroot="$YDB_SDK" -isysroot "$YDB_SDK" \
    -nostdinc++ -std=c++20 \
    -I "$PWD/contrib/libs/cxxsupp/libcxx/include" \
    -I "$PWD/contrib/libs/cxxsupp/libcxxrt/include" \
    -I "$PWD/contrib/libs/clang20-rt/include" \
    -x c++ -fsyntax-only - \
  && echo 'Clang 20 + selected SDK: OK'
```

Continue only after this prints `Clang 20 + selected SDK: OK`. This checks the compiler and headers directly; it does not verify which SDK a subsequent `ya make` invocation selects.

## 5. Build the server

Run from the repository root. The example starts with four parallel jobs; increase `YDB_BUILD_JOBS` if memory permits. Sustained swapping can make more jobs slower.

```bash
YDB_BUILD_JOBS=4
YDB_SDK_FLAGS="--sysroot=$YDB_SDK -isysroot $YDB_SDK"

./ya make ydb/apps/ydbd -k -j "$YDB_BUILD_JOBS" --build relwithdebinfo \
  -DNO_SPLIT_DWARF=yes \
  -DCFLAGS="$YDB_SDK_FLAGS" \
  -DLDFLAGS="$YDB_SDK_FLAGS" \
  --host-platform-flag="CFLAGS=$YDB_SDK_FLAGS" \
  --host-platform-flag="LDFLAGS=$YDB_SDK_FLAGS"
```

The SDK flags cover both the target and the host tools built during compilation. On Darwin, exporting `SDKROOT` alone is insufficient for this `ya` configuration. Adding only `--sysroot` can still leave the compiler using headers selected through `SDKROOT`. Supplying both `--sysroot` and `-isysroot` selects the intended SDK for headers and linking.

On Darwin, `-DNO_SPLIT_DWARF=yes` skips the separate `dsymutil` step that produces a `.dSYM` bundle; it does not disable compilation with debug information. Omit it if you need that bundle.

The first build downloads toolchains and compiles dependencies, so it can take hours. Keep the cache for subsequent builds. To build the CLI, repeat the same command with `ydb/apps/ydb` instead of `ydb/apps/ydbd`.

The variables above apply only to the current terminal session. In a new terminal, restore them before repeating the build command (adjust the SDK path and job count if needed):

```bash
cd "$HOME/ydbwork/ydb"
export YA_CACHE_DIR="$HOME/ydbwork/.ya"
YDB_SDK=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk
YDB_SDK_FLAGS="--sysroot=$YDB_SDK -isysroot $YDB_SDK"
YDB_BUILD_JOBS=4
```

After the build exits successfully, check the executable:

```bash
file -L ydb/apps/ydbd/ydbd
./ydb/apps/ydbd/ydbd --help
```

The server should be an ARM64 Mach-O executable. The corresponding CLI output path is `ydb/apps/ydb/ydb`.

Keep the same SDK flags when building test targets. See [Build and test YDB using Ya Make](ydb/docs/en/core/contributor/build-ya.md) for selecting a test target and filter; this recipe does not establish that the entire test suite passes on macOS.

## Troubleshooting

- **`origin/main` or `origin` does not exist:** this recipe uses `official`. Inspect `git remote -v` before adapting commands to an existing checkout.
- **`INFINITY` is undeclared in Arrow or libc++ `<random>`:** rerun step 4 and inspect the failing compiler command for the explicit `-isysroot` pointing to SDK 26.5. To collect diagnostic output from the build, add `--verbose --log-file="$PWD/macos-build.log"` to the step 5 command and inspect the compiler invocations in that log. SDK 27 with Clang 20 reproduced this error. An Xcode switch or an exported `SDKROOT` is not sufficient evidence that the build uses the desired headers.
- **`lld22.json`, `clang22-rt`, or sanitizer headers are missing after experimenting with Clang 22:** start from the Clang 20 commands above. Remove the experimental `clang22-*`, `USE_ARCADIA_COMPILER_RUNTIME=no`, and `LLD_VERSION` overrides from the command and any local configuration before retrying.
- **An SDK path exists but `xcrun` rejects it:** check `SDKSettings.json`, `usr/include/math.h`, and the direct compiler probe. Do not replace or redirect system SDK symlinks to make this recipe work.

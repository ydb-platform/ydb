<!-- SPDX-FileCopyrightText: 2026 geisserml <geisserml@gmail.com> -->
<!-- SPDX-License-Identifier: CC-BY-4.0 -->

# pypdfium2

<!-- [![Downloads](https://pepy.tech/badge/pypdfium2/month)](https://pepy.tech/project/pypdfium2) -->

pypdfium2 is an ABI-level Python 3 binding to [PDFium](https://pdfium.googlesource.com/pdfium/+/refs/heads/main), a powerful and liberal-licensed library for PDF rendering, inspection, manipulation and creation.

It is built with [ctypesgen](https://github.com/pypdfium2-team/ctypesgen) and external [PDFium binaries](https://github.com/bblanchon/pdfium-binaries/).
The custom setup infrastructure provides a seamless packaging and installation process. A wide range of platforms is supported with pre-built packages.

pypdfium2 includes [helpers](#support-model) to simplify common use cases, while the [raw PDFium API](#raw-pdfium-api) (ctypes) remains accessible as well.


## Installation

### From PyPI (recommended)
```bash
python -m pip install -U pypdfium2
```
If available for your platform, this will use a pre-built wheel package, which is the easiest way of installing pypdfium2.
Otherwise, [setup code](#from-the-repository--with-setup) will run.
If your platform is not covered with pre-built binaries, this will look for system pdfium, or attempt to build pdfium from source.

#### JavaScript/XFA builds

pdfium-binaries also offer V8 (JavaScript) / XFA enabled builds.
If you need them, do e.g.:
```bash
PDFIUM_PLATFORM=auto-v8 pip install -v pypdfium2 --no-binary pypdfium2
```
This will bypass wheels and run setup, while requesting use of V8 builds through the `PDFIUM_PLATFORM=auto-v8` environment setting. See below for more info.

#### Optional runtime dependencies

As of this writing, pypdfium2 does not require any mandatory runtime dependencies, apart from Python and PDFium itself (which is commonly bundled).

However, some optional support model / CLI features need additional packages:
* [`Pillow`](https://pillow.readthedocs.io/en/stable/) (module `PIL`) is a pouplar imaging library for Python. pypdfium2 provides convenience adapters to translate between raw bitmap buffers and PIL images. It also uses PIL for some command-line functionality (e.g. image saving).
* [`NumPy`](https://numpy.org/doc/stable/index.html) is a library for scientific computing. As with `Pillow`, pypdfium2 provides helpers to get a numpy array view of a raw bitmap.
* [`opencv-python`](https://github.com/opencv/opencv-python) (module `cv2`) is an imaging library built around numpy arrays. It can be used in the rendering CLI to save with pypdfium2's numpy adapter.

pypdfium2 tries to defer imports of optional dependencies until they are actually needed, so there should be no startup overhead if you don't use them.


### From the repository / With setup

_Note, unlike helpers, pypdfium2's setup is not bound by API stability promises, so it may change any time._

#### Setup Dependencies

*System*
+ C pre-processor (`gcc`/`clang` – alternatively, specify the command to invoke via `$CPP`)
+ `git` (Used e.g. to determine the latest pdfium-binaries version, to get `git describe` info, or to check out pdfium on sourcebuild. Might be optional on default setup.)
+ [`gh >= 2.47.0`](https://github.com/cli/cli/) (optional; used to verify pdfium-binaries build attestations)

*Python*
+ [`ctypesgen` (pypdfium2-team fork)](https://github.com/pypdfium2-team/ctypesgen)
+ `setuptools`
+ `wheel`, if setuptools is `< v70.1.0`

Python dependencies should be installed automatically, unless `--no-build-isolation` is passed to pip.

> [!NOTE]
> pypdfium2 and its ctypesgen fork are developed in sync, i.e. each pypdfium2 commit ought to be coupled with the then `HEAD` of pypdfium2-ctypesgen.<br>
> Our release sdists, and latest pypdfium2 from git, will automatically use matching ctypesgen.<br>
> However, when using a non-latest commit, you'll have to set up the right ctypesgen version on your own, and install pypdfium2 without build isolation.

#### Get the code

```bash
git clone "https://github.com/pypdfium2-team/pypdfium2.git"
cd pypdfium2/
```

#### Default setup

```bash
# In the pypdfium2/ directory
python -m pip install -v .
```

This will invoke pypdfium2's `setup.py`. Typically, this means a binary will be downloaded from `pdfium-binaries` and bundled into pypdfium2, and ctypesgen will be called on pdfium headers to produce the bindings interface.

`pdfium-binaries` offer GitHub build provenance [attestations](https://github.com/bblanchon/pdfium-binaries/attestations), so it is highly recommended that you install the `gh` CLI for our setup to verify authenticity of the binaries.

If no pre-built binaries are available for your platform, setup will [look for system pdfium](#with-system-pdfium), or attempt to [build pdfium from source](#with-self-built-pdfium).

##### `pip` options of interest

- `-v`: Verbose logging output. Useful for debugging.
- `-e`: Install in editable mode, so the installation points to the source tree. This way, changes directly take effect without needing to re-install. Recommended for development.
- `--no-build-isolation`: Do not isolate setup in a virtual env; use the main env instead. This renders `pyproject.toml [build-system]` inactive, so setup deps must be prepared by caller. Useful to install custom versions of setup deps, or as speedup when installing repeatedly.
- `--no-binary pypdfium2`: Do not use binary *wheels* when installing from PyPI – instead, use the sdist and run setup. Note, this option is improperly named, as pypdfium2's setup will attempt to use binaries all the same. If you want to prevent that, set e.g. `PDFIUM_PLATFORM=fallback` to achieve the same behavior as if there were no pdfium-binaries for the host. Or if you just want to package a source distribution, set `PDFIUM_PLATFORM=sdist`.
- `--pre` to install a beta release, if available.


#### With system pdfium

```bash
PDFIUM_PLATFORM="system-search" python -m pip install -v .
```

Look for a system-provided pdfium shared library, and bind against it.

Standard, portable [`ctypes.util.find_library()`](https://docs.python.org/3/library/ctypes.html#finding-shared-libraries) means will be used to probe for system pdfium at setup time, and the result will be hardcoded into the bindings. Alternatively, set `$PDFIUM_BINARY` to the path of the out-of-tree DLL to use.

If system pdfium was found, we will look for pdfium headers from which to generate the bindings (e.g. in `/usr/include`). If the headers are in a location not recognized by our code, set `$PDFIUM_HEADERS` to the directory in question.

Also, we try to determine the pdfium version, either from the library filename itself, or via `pkg-config`.
If this fails, you can pass the version alongside the setup target, e.g. `PDFIUM_PLATFORM=system-search:XXXX`, where `XXXX` is the pdfium build version.
If the version is not known in the end, `NaN` placeholders will be set.

If the version is known but no headers were found, they will be downloaded from upstream.
If neither headers nor version are known (or ctypesgen is not installed), the reference bindings will be used as a last resort. This is ABI-unsafe and thus discouraged.

If `find_library()` failed to find pdfium, we *may* do additional, custom search, such as checking for a pdfium shared library included with LibreOffice, and – if available – determining its version.<br>
Our search heuristics currently expect a Linux-like filesystem hierarchy (e.g. `/usr`), but contributions for other systems are welcome.

> [!IMPORTANT]
> When pypdfium2 is installed with system pdfium, the bindings ought to be re-generated with the new headers whenever the out-of-tree pdfium DLL is updated, for ABI safety reasons.[^upstream_abi_policy]<br>
> For distributors, we highly recommend the use of versioned libraries (e.g. `libpdfium.so.140.0.7269.0`) or similar concepts that enforce binary/bindings version match, so outdated bindings will safely stop working with a meaningful error, rather than silently continue unsafely, at risk of hard crashes.

> [!TIP]
> If you mind pypdfium2's setup making a web request to resolve the full version, you may pass it in manually via `GIVEN_FULLVER=$major.$minor.$build.$patch` (colon-separated if there are multiple versions), or less ideally, set `IGNORE_FULLVER=1` to use `NaN` placeholders.
> This applies to other setup targets as well.<br>
> For distributors, we recommend that you use the full version in binary filename or pkgconfig info, so pypdfium2's setup will not need to resolve it in the first place.

[^upstream_abi_policy]: Luckily, upstream tend to be careful not to change the ABI of existing stable APIs, but they don't mind ABI-breaking changes to APIs that have not been promoted to stable tier yet, and pypdfium2 uses many of them, so it is still prudent to care about downstream ABI safety as well (it always is). You can read more about upstream's policy [here](https://pdfium.googlesource.com/pdfium/+/refs/heads/main/CONTRIBUTING.md#stability).

##### Related targets

There is also a `system-generate:$VERSION` target, to produce system pdfium bindings in a host-independent fashion. This will call `find_library()` at runtime, and may be useful for packaging.

Further, you can set just `system` to consume pre-generated files from the `data/system` staging directory. See the section on [caller-provided data files](#with-caller-provided-data-files) for more info.


#### With self-built pdfium

You can also install pypdfium2 with a self-compiled pdfium shared library, by placing it in `data/sourcebuild/` along with a bindings interface and version info, and setting the `PDFIUM_PLATFORM="sourcebuild"` directive to use these files on setup.

This project comes with two scripts to automate the build process: `build_toolchained.py` and `build_native.py` (in `setupsrc/`).
- `build_toolchained` is based on the build instructions in pdfium's Readme, and uses Google's toolchain (this means foreign binaries and sysroots). This results in a heavy checkout process that may take a lot of time and space. Dependency libraries are vendored. An advantage of the toolchain is its powerful cross-compilation support (including symbol reversioning).
- `build_native` is an attempt to address some shortcomings of the toolchained build. It performs a lean, self-managed checkout, and is tailored towards native compilation. It uses system dependencies (compiler/gn/ninja), which must be installed by the caller beforehand. This script should theoretically work on arbitrary Linux architectures. As a drawback, this process is not supported or even documented upstream, so it might be hard to maintain.

> [!TIP]
> The native sourcebuild can either use system libraries, or pdfium's vendored libraries.
> When invoked directly, by default, system libraries need to be installed. However, when invoked through fallback setup (`PDFIUM_PLATFORM=fallback`), vendored libraries will be used.<br>
> The `--vendor ...` and `--no-vendor ...` options can be used to control vendoring on a per-library basis. See `build_native.py --help` for details.

You can also set `PDFIUM_PLATFORM` to `sourcebuild-native` or `sourcebuild-toolchained` to trigger either build script through setup, and pass command-line flags with `$BUILD_PARAMS`.
However, for simplicity, both scripts/subtargets share just `sourcebuild` as staging directory.

Dependencies:
- When building with system libraries, the following packages need to be installed (including development headers): `freetype, icu-uc, lcms2, libjpeg, libopenjp2, libpng, libtiff, zlib` (and maybe `glib` to satisfy the build system).
- You might also want to know that pdfium bundles `agg, abseil, fast_float`.
- When building with system tools, `gn (generate-ninja)`, `ninja`, and a compiler are needed. If available, the compiler defaults to GCC, but Clang should also work if you set up some symlinks, and make sure you have the `libclang_rt` builtins or pass `--no-libclang-rt`.

To do the toolchained build, you'd run something like:
```bash
# call build script with --help to list options
python setupsrc/build_toolchained.py
PDFIUM_PLATFORM="sourcebuild" python -m pip install -v .
```

Or for the native build, on Ubuntu 24.04, you could do e.g.:
```bash
# Install dependencies
sudo apt-get install generate-ninja ninja-build libfreetype-dev liblcms2-dev libjpeg-dev libopenjp2-7-dev libpng-dev libtiff-dev zlib1g-dev libicu-dev libglib2.0-dev
```
```bash
# Build with GCC
python ./setupsrc/build_native.py --compiler gcc
```
```bash
# Alternatively, build with Clang
sudo apt-get install llvm lld
VERSION=18
ARCH=$(uname -m)
sudo ln -s "/usr/lib/clang/$VERSION/lib/linux" "/usr/lib/clang/$VERSION/lib/$ARCH-unknown-linux-gnu"
sudo ln -s "/usr/lib/clang/$VERSION/lib/linux/libclang_rt.builtins-$ARCH.a" "/usr/lib/clang/$VERSION/lib/linux/libclang_rt.builtins.a"
python ./setupsrc/build_native.py --compiler clang
```
```bash
# Install
PDFIUM_PLATFORM="sourcebuild" python -m pip install -v .
```

Note, on *some* platforms, you might also need symlinks for GCC, e.g.:
```bash
PREFIX=$(python ./utils/get_gcc_prefix.py)  # in pypdfium2 dir
GCC_DIR="/usr"  # or e.g. /opt/rh/gcc-toolset-14/root
sudo ln -s $GCC_DIR/bin/gcc $GCC_DIR/bin/$PREFIX-gcc
sudo ln -s $GCC_DIR/bin/g++ $GCC_DIR/bin/$PREFIX-g++
sudo ln -s $GCC_DIR/bin/nm $GCC_DIR/bin/$PREFIX-nm
sudo ln -s $GCC_DIR/bin/readelf $GCC_DIR/bin/$PREFIX-readelf
sudo ln -s $GCC_DIR/bin/ar $GCC_DIR/bin/$PREFIX-ar
```

> [!NOTE]
> The native sourcebuild currently supports Linux (or similar).
> macOS and Windows are not handled, as we do not have access to these systems, and working over CI did not turn out feasible – use the toolchain-based build for now.
> Community help / pull requests to extend platform support would be welcome.

##### Android (Termux)

The native build may also work on Android with Termux in principle.

<details>
<summary>Click to expand for instructions</summary>

First, make sure git can work in your checkout of pypdfium2:
```bash
# set $PROJECTS_FOLDER accordingly
git config --global --add safe.directory '$PROJECTS_FOLDER/*'
```

To install the dependencies, you'll need something like
```bash
pkg install gn ninja freetype littlecms libjpeg-turbo openjpeg libpng zlib libicu libtiff glib
```

Then apply the clang symlinks as described above, but use `ARCH=$(uname -m)-android`
and substitute `/usr` with `$PREFIX` (`/data/data/com.termux/files/usr`).

Last time we tested `build_native` on Android, there were some bugs with freetype/openjpeg includes. A *quick & dirty* workaround with symlinks is:
```bash
# freetype
ln -s "$PREFIX/include/freetype2/ft2build.h" "$PREFIX/include/ft2build.h"
ln -s "$PREFIX/include/freetype2/freetype" "$PREFIX/include/freetype"

# openjpeg
OPJ_VER="2.5"  # adapt this to your setup
ln -s "$PREFIX/include/openjpeg-$OPJ_VER/openjpeg.h" "$PREFIX/include/openjpeg.h"
ln -s "$PREFIX/include/openjpeg-$OPJ_VER/opj_config.h" "$PREFIX/include/opj_config.h"
```

Now, you should be ready to run the build.

On Android, PDFium's build system outputs `libpdfium.cr.so` by default, thus you'll want to rename the binary so pypdfium2's library search can find it:
```bash
mv data/sourcebuild/libpdfium.cr.so data/sourcebuild/libpdfium.so
```
Then install with `PDFIUM_PLATFORM=sourcebuild`.

In case dependency libraries were built separately, you may also need to set the OS library search path, e.g.:
```bash
PY_VERSION="3.12"  # adapt this to your setup
LD_LIBRARY_PATH="$PREFIX/lib/python$PY_VERSION/site-packages/pypdfium2_raw"
```
By default, our build script currently bundles everything into a single DLL, though.

</details>

##### cibuildwheel

Sourcebuild can be run through cibuildwheel. For targets configured in our [`pyproject.toml`](./pyproject.toml), the basic invocation is as simple as p.ex.
```bash
CIBW_BUILD="cp311-manylinux_x86_64" cibuildwheel
```
A more involved use case could look like this:
```bash
CIBW_BUILD="cp310-musllinux_s390x" CIBW_ARCHS=s390x CIBW_CONTAINER_ENGINE=podman TEST_PDFIUM=1 cibuildwheel
```
See also our [cibuildwheel](.github/workflows/cibw.yaml) [workflow](.github/workflows/cibw_one.yaml).
For more options, see the [upstream documentation](https://cibuildwheel.pypa.io/en/stable/options).

On Linux, this will use the native sourcebuild with vendored dependency libraries.
On Windows and macOS, the toolchained sourcebuild is used.

Note, for Linux, cibuildwheel requires Docker. On the author's version of Fedora, it can be installed as follows:
```bash
sudo dnf in moby-engine  # this provides the docker command
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -aG docker $USER
# then reboot (re-login might also suffice)
```
For other ways of installing Docker, refer to the cibuildwheel docs ([Setup](https://cibuildwheel.pypa.io/en/stable/setup/), [Platforms](https://cibuildwheel.pypa.io/en/stable/platforms/)) and the links therein.

> [!WARNING]
> cibuildwheel copies the project directory into a container, not taking `.gitignore` rules into account.
> Thus, it is advisable to make a fresh checkout of pypdfium2 before running cibuildwheel.
> In particular, a toolchained checkout of pdfium within pypdfium2 is problematic, and will cause a halt on the `Copying project into container...` step.
> For development, make sure the fresh checkout is in sync with the working copy.

> [!TIP]
> pdfium itself has first-class cross-compilation support.
> In particular, for Linux architectures supported by upstream's toolchain but not available natively on CI, we recommend to forego cibuildwheel, and instead cross-build pdfium using its own toolchain, e.g.:
> ```bash
> # assuming cross-compilation dependencies are installed
> python setupsrc/build_toolchained.py --target-cpu arm
> PDFIUM_PLATFORM=sourcebuild CROSS_TAG="manylinux_2_17_armv7l" python -m build -wxn
> ```
> This typically achieves a lower glibc requirement than we can with cibuildwheel.


#### With caller-provided data files

pypdfium2 is like any other Python project in essentials, except that it needs some data files: a pdfium DLL (either bundled or out-of-tree), a bindings interface (generated via ctypesgen), and pdfium version info (JSON).

The main point of pypdfium2's custom setup is to automate deployment of these files, in a way that suits end users / contributors, and our PyPI packaging.

However, if you want to (or have to) forego this automation, you can also *just supply these files yourself*, as shown below. This allows to largely sidestep pypdfium2's own setup code.<br>
The idea is basically to put your data files in a staging directory, `data/sourcebuild` or `data/system` (depending on whether you want to bundle or use system pdfium), and set the matching `$PDFIUM_PLATFORM` target to consume from that directory on setup.

This setup strategy should be inherently free of web requests.
Mind though, we don't support the result. If you bring your own files, it's your own responsibility, and it's quite possible your pypdfium2 might turn out subtly different from ours.

```bash
# First, ask yourself: Do you want to bundle pdfium (in-tree), or use system
# pdfium (out-of-tree)? For bundling, set "sourcebuild", else set "system".
TARGET="sourcebuild"  # or "system"
STAGING_DIR="data/$TARGET"

# If you have decided for bundling, copy over the pdfium DLL in question.
# Otherwise, skip this step.
cp "$MY_BINARY_PATH" "$STAGING_DIR/libpdfium.so"

# Now, we will call ctypesgen to generate the bindings interface.
# Reminder: You'll want to use the pypdfium2-team fork of ctypesgen.
# It generates much cleaner bindings, and it's what our source expects
# (there may be subtle API differences in terms of output).
# How exactly you do this is down to you.
# See ctypesgen --help or base.py::run_ctypesgen() for further options.
ctypesgen --library pdfium --rt-libpaths $MY_RT_LIBPATHS --ct-libpaths $MY_CT_LIBPATHS \
--headers $MY_INCLUDE_DIR/fpdf*.h -o $STAGING_DIR/bindings.py [-D $MY_RAW_FLAGS]

# Then write the version file (fill the placeholders).
# Note, this is not a mature interface yet and might change any time!
# See also https://pypdfium2.readthedocs.io/en/stable/python_api.html#pypdfium2.version.PDFIUM_INFO
# major/minor/build/patch: integers forming the pdfium version being packaged
# n_commits/hash: git describe like post-tag info (0/null for release commit)
# origin: a string to identify the build
# flags: a comma-delimited list of pdfium feature flag strings
#        (e.g. "V8", "XFA") - may be empty for default build
cat > "$STAGING_DIR/version.json" <<END
{
  "major": $PDFIUM_MAJOR,
  "minor": $PDFIUM_MINOR,
  "build": $PDFIUM_BUILD,
  "patch": $PDFIUM_PATCH,
  "n_commits": $POST_TAG_COMMIT_COUNT,
  "hash": $POST_TAG_HASH,
  "origin": "$TARGET-$MY_ORIGIN",
  "flags": [$MY_SHORT_FLAGS]
}
END

# Finally, run setup (through pip, pyproject-build or whatever).
# The PDFIUM_PLATFORM value will instruct pypdfium2's setup to use the files
# we supplied, rather than to generate its own.
PDFIUM_PLATFORM=$TARGET python -m pip install --no-build-isolation -v .
```


#### Further setup info (formal summary)

This is a *somewhat* formal description of pypdfium2's setup capabilities.
It is meant to sum up and complement the above documentation on specific sub-targets.

Disclaimer: As it is hard to keep up with constantly evolving setup code, it is possible this documentation may be outdated/incomplete. Also keep in mind that these APIs could change any time, and may be mainly of internal interest.

* Binaries are stored in platform-specific sub-directories of `data/`, along with bindings and version information.

* `$PDFIUM_PLATFORM` defines which binary to include on setup.
  - Format spec: `[$PLATFORM][-v8][:$VERSION]` (`[]` = segments, `$CAPS` = variables).
  - Examples: `auto`, `auto:7269` `auto-v8:7269` (`auto` may be substituted by an explicit platform name, e.g. `linux_x64`).
  - V8: If given, use the V8 (JavaScript) and XFA enabled pdfium binaries. Otherwise, use the regular (non-V8) binaries.
  - Version: If given, use the specified pdfium-binaries release. Otherwise, use the default version currently set in the codebase. Set `pinned` to request that behavior explicitly. Or set `latest` to use the newest pdfium-binaries release instead.
  - Platform:
    + If unset or `auto`, the host platform is detected and a corresponding binary will be selected.
    + If an explicit platform identifier (e.g. `linux_x64`, `darwin_arm64`, ...), binaries for the requested platform will be used.[^platform_ids]
    + If `system-search`, look for and bind against system-provided pdfium instead of embedding a binary. If just `system`, consume existing bindings from `data/system/`.
    + If `sourcebuild`, binary and bindings will be taken from `data/sourcebuild/`, assuming a prior run of the native or toolchained build scripts. `sourcebuild-native` or `sourcebuild-toolchained` can also be used to trigger either build through setup (use `$BUILD_PARAMS` to pass custom options).
    + If `sdist`, no platform-specific files will be included, so as to create a source distribution.

* `$PYPDFIUM_MODULES=[raw,helpers]` defines the modules to include. Metadata adapts dynamically.
  - May be used by packagers to decouple raw bindings and helpers, which may be relevant if packaging against system pdfium.
  - Would also allow to install only the raw module without helpers, or only helpers with a custom raw module.

* `$PDFIUM_BINDINGS=reference` allows to override ctypesgen and use the reference bindings file `autorelease/bindings.py` instead.
  - This is a convenience option to get pypdfium2 installed from source even if a working ctypesgen / C pre-processor is not available in the install env. *May be automatically enabled under given circumstances.*
  - Warning: This may not be ABI-safe. Please make sure binary/bindings build headers match to avoid ABI issues.

[^platform_ids]: Intended for packaging, so that wheels can be crafted for any platform without access to a native host.


### From Conda

> [!WARNING]
> **Beware:** Any conda packages/recipes of pypdfium2 or pdfium-binaries that might be provided by other distributors, including `anaconda/main` or `conda-forge` default channels, are [unofficial](#unofficial-packages).

> [!NOTE]
> **Wait a moment:** Do you really need this?
> pypdfium2 is best installed from `PyPI` (e.g. via `pip`),[^pypi_reasons] which you can also do in a conda env. Rather than asking your users to add custom channels, consider making pypdfium2 optional at install time, and ask them to install it via pip instead.<br>
> This library has no hard runtime dependencies, so you don't need to worry about breaking the conda env.

[^pypi_reasons]: To name some reasons:
    + pypdfium2 from PyPI covers platforms that we cannot cover on conda.
    + pypdfium2 from PyPI has extensive fallback setup, while conda does not provide an opportunity to run custom setup code.
    + With conda, in-project publishing / custom channels are second class.
    + With conda, it seems there is no way to create platform-specific but interpreter-independent python packages, so we cannot reasonably bundle pdfium. Thus, we have to use external pdfium, which is more complex and has some pitfalls.

+ To install
  
  With permanent channel config (encouraged):
  ```bash
  conda config --add channels bblanchon
  conda config --add channels pypdfium2-team
  conda config --set channel_priority strict
  conda install pypdfium2-team::pypdfium2_helpers
  ```
  
  Alternatively, with temporary channel config:
  ```bash
  conda install pypdfium2-team::pypdfium2_helpers --override-channels -c pypdfium2-team -c bblanchon -c defaults
  ```
  
  If desired, you may limit the channel config to the current environment by adding `--env`.
  Adding the channels permanently and tightening priority is encouraged to include pypdfium2 in `conda update` by default, and to avoid accidentally replacing the install with a different channel.
  Otherwise, you should be cautious when making changes to the environment.

+ To depend on pypdfium2 in a `conda-build` recipe
  ```yaml
  requirements:
    run:
      - pypdfium2-team::pypdfium2_helpers
  ```
  You'll want to have downstream callers handle the custom channels as shown above, otherwise conda will not be able to satisfy requirements.

+ To set up channels in a GH workflow
  ```yaml
  - name: ...
    uses: conda-incubator/setup-miniconda@v3
    with:
      # ... your options
      channels: pypdfium2-team,bblanchon
      channel-priority: strict
  ```
  This is just a suggestion, you can also call `conda config` manually, or pass channels on command basis using `-c`, as discussed above.

+ To verify the sources
  ```bash
  conda list --show-channel-urls "pypdfium2|pdfium-binaries"
  conda config --show-sources
  ```
  The table should show `pypdfium2-team` and `bblanchon` in the channels column.
  If added permanently, the config should also include these channels, ideally with top priority.
  Please check this before reporting any issue with a conda install of pypdfium2.

_**Note:** Conda packages are normally managed using recipe feedstocks driven by third parties, in a Linux repository like fashion. However, with some quirks it is also possible to do conda packaging within the original project and publish to a custom channel, which is what pypdfium2-team does, and the above instructions are referring to._


### Unofficial packages

The authors of this project have no control over and are not responsible for possible third-party builds of pypdfium2, and we do not support them. Please use our official packages where possible.
If you have an issue with a third-party build, either contact your distributor, or try to reproduce with our official builds.

Do not expect us to add/change code for downstream-specific setup tasks.
Related issues or PRs may be closed without further notice if we don't see fit for upstream.
Enhancements of general value that are maintainable and align well with the idea of our setup code are welcome, though.

> [!IMPORTANT]
> If you are a third-party distributor, please point out in the description that your package is unofficial, i.e. not affiliated with or endorsed by the pypdfium2 authors.<br>
> In particular, if you feel like you need patches to package pypdfium2, please submit them on the Discussions page so we can figure out if there isn't a better way (there usually is).


## Usage

### [Support model](https://pypdfium2.readthedocs.io/en/stable/python_api.html)

<!-- TODO demonstrate more APIs (e. g. XObject placement, transform matrices, image extraction, ...) -->

Here are some examples of using the support model API.

* Import the library
  ```python
  import pypdfium2 as pdfium
  import pypdfium2.raw as pdfium_c
  ```

* Open a PDF using the helper class `PdfDocument` (supports file paths as string or `pathlib.Path`, or file content as bytes or byte stream)
  ```python
  pdf = pdfium.PdfDocument("./path/to/document.pdf")
  version = pdf.get_version()  # get the PDF standard version
  n_pages = len(pdf)  # get the number of pages in the document
  page = pdf[0]  # load a page
  ```

* Render the page
  ```python
  bitmap = page.render(
      scale = 1,    # 72dpi resolution
      rotation = 0, # no additional rotation
      # ... further rendering options
  )
  pil_image = bitmap.to_pil()
  pil_image.show()
  ```
  
  Note, with the PIL adapter, it might be advantageous to use `force_bitmap_format=pdfium_c.FPDFBitmap_BGRA, rev_byteorder=True` or perhaps `prefer_bgrx=True, maybe_alpha=True, rev_byteorder=True`, to achieve a pixel format supported natively by PIL, and avoid rendering with transparency to a non-alpha bitmap, which can slow down pdfium.
  
  With `.to_numpy()`, all formats are zero-copy, but passing either `maybe_alpha=True` (if dynamic pixel format is acceptable) or `force_bitmap_format=pdfium_c.FPDFBitmap_BGRA` is also recommended for the transparency problem.

* Try some page methods
  ```python
  # Get page dimensions in PDF canvas units (1pt->1/72in by default)
  width, height = page.get_size()
  # Set the absolute page rotation to 90° clockwise
  page.set_rotation(90)
  
  # Locate objects on the page
  for obj in page.get_objects():
      print(obj.level, obj.type, obj.get_bounds())
  ```

* Extract and search text
  ```python
  # Load a text page helper
  textpage = page.get_textpage()
  
  # Extract text from the whole page
  text_all = textpage.get_text_bounded()
  # Extract text from a specific rectangular area
  text_rect = textpage.get_text_bounded(left=50, bottom=100, right=width-50, top=height-100)
  # Extract text from a specific char range
  text_span = textpage.get_text_range(index=10, count=15)
  
  # Locate text on the page
  searcher = textpage.search("something", match_case=False, match_whole_word=False)
  # This returns the next occurrence as (char_index, char_count), or None if not found
  match = searcher.get_next()
  ```

* Read the table of contents
  ```python
  import pypdfium2.internal as pdfium_i
  
  for bm in pdf.get_toc(max_depth=15):
      count, dest = bm.get_count(), bm.get_dest()
      out = "    " * bm.level
      out += "[%s] %s -> " % (
          f"{count:+}" if count != 0 else "*",
          bm.get_title(),
      )
      if dest:
          index, (view_mode, view_pos) = dest.get_index(), dest.get_view()
          out += "%s  # %s %s" % (
              index+1 if index != None else "?",
              pdfium_i.ViewmodeToStr.get(view_mode),
              round(view_pos, 3),
          )
      else:
          out += "_"
      print(out)
  ```

* Create a new PDF with an empty A4 sized page
  ```python
  pdf = pdfium.PdfDocument.new()
  width, height = (595, 842)
  page_a = pdf.new_page(width, height)
  ```

* Include a JPEG image in a PDF
  ```python
  pdf = pdfium.PdfDocument.new()
  
  image = pdfium.PdfImage.new(pdf)
  image.load_jpeg("./tests/resources/mona_lisa.jpg")
  width, height = image.get_px_size()
  
  matrix = pdfium.PdfMatrix().scale(width, height)
  image.set_matrix(matrix)
  
  page = pdf.new_page(width, height)
  page.insert_obj(image)
  page.gen_content()
  ```

* Save the document
  ```python
  # PDF 1.7 standard
  pdf.save("output.pdf", version=17)
  ```

### Raw PDFium API

While helper classes conveniently wrap the raw PDFium API, it may still be accessed directly and is available in the namespace `pypdfium2.raw`. Lower-level utilities that may aid with using the raw API are provided in `pypdfium2.internal`.

```python
import pypdfium2.raw as pdfium_c
import pypdfium2.internal as pdfium_i
```

Since PDFium is a large library, many components are not covered by helpers yet. However, as helpers expose their underlying raw objects, you may seamlessly integrate raw APIs while using helpers as available. When passed as ctypes function parameter, helpers automatically resolve to the raw object handle (but you may still access it explicitly if desired):
```python
permission_flags = pdfium_c.FPDF_GetDocPermission(pdf.raw)  # explicit
permission_flags = pdfium_c.FPDF_GetDocPermission(pdf)      # implicit
```

For PDFium docs, please look at the comments in its [public header files](https://pdfium.googlesource.com/pdfium/+/refs/heads/main/public/).[^pdfium_docs]
A variety of examples on how to interface with the raw API using [`ctypes`](https://docs.python.org/3/library/ctypes.html) is already provided with [support model source code](src/pypdfium2/_helpers).
Nonetheless, the following guide may be helpful to get started with the raw API, if you are not familiar with `ctypes` yet.

[^pdfium_docs]: Unfortunately, no recent HTML-rendered docs are available for PDFium at the moment.

<!-- TODO write something about weakref.finalize(); add example on creating a C page array -->
<!-- TODO doctests? -->

* In general, PDFium functions can be called just like normal Python functions.
  However, parameters may only be passed positionally, i.e. it is not possible to use keyword arguments.
  There are no defaults, so you always need to provide a value for each argument.
  ```python
  # arguments: filepath (bytes), password (bytes|None)
  # NUL-terminate filepath and encode as UTF-8
  pdf = pdfium_c.FPDF_LoadDocument((filepath+"\x00").encode("utf-8"), None)
  ```
  This is the underlying bindings declaration,[^bindings_decl] which loads the function from the binary and
  contains the information required to convert Python types to their C equivalents.
  ```python
  if hasattr(_libs['pdfium'], 'FPDF_LoadDocument'):
      FPDF_LoadDocument = _libs['pdfium']['FPDF_LoadDocument']
      FPDF_LoadDocument.argtypes = (FPDF_STRING, FPDF_BYTESTRING)
      FPDF_LoadDocument.restype = FPDF_DOCUMENT
  ```
  Python `bytes` are converted to `FPDF_STRING` by ctypes autoconversion. This works because `FPDF_STRING` is actually an alias to `POINTER(c_char)` (i.e. `char*`), which is a primitive pointer type.
  When passing a string to a C function, it must always be NUL-terminated, as the function merely receives a pointer to the first item and then continues to read memory until it finds a NUL terminator.
  
[^bindings_decl]: From the auto-generated bindings file. We maintain a reference copy at `autorelease/bindings.py`. Or if you have an editable install, there will also be `src/pypdfium2_raw/bindings.py`.

* First of all, function parameters are not only used for input, but also for output:
  ```python
  # Initialise an integer object (defaults to 0)
  c_version = ctypes.c_int()
  # Let the function assign a value to the c_int object, and capture its return code (True for success, False for failure)
  ok = pdfium_c.FPDF_GetFileVersion(pdf, c_version)
  # If successful, get the Python int by accessing the `value` attribute of the c_int object
  # Otherwise, set the variable to None (in other cases, it may be desired to raise an exception instead)
  version = c_version.value if ok else None
  ```

* If an array is required as output parameter, you can initialise one like this (in general terms):
  ```python
  # long form
  array_type = (c_type * array_length)
  array_object = array_type()
  # short form
  array_object = (c_type * array_length)()
  ```
  Example: Getting view mode and target position from a destination object returned by some other function.
  ```python
  # (Assuming `dest` is an FPDF_DEST)
  n_params = ctypes.c_ulong()
  # Create a C array to store up to four coordinates
  view_pos = (pdfium_c.FS_FLOAT * 4)()
  view_mode = pdfium_c.FPDFDest_GetView(dest, n_params, view_pos)
  # Convert the C array to a Python list and cut it down to the actual number of coordinates
  view_pos = list(view_pos)[:n_params.value]
  ```

* For string output parameters, callers needs to provide a sufficiently long, pre-allocated buffer.
  This may work differently depending on what type the function requires, which encoding is used, whether the number of bytes or characters is returned, and whether space for a NUL terminator is included or not. Carefully review the documentation of the function in question to fulfill its requirements.
  
  Example A: Getting the title string of a bookmark.
  ```python
  # (Assuming `bookmark` is an FPDF_BOOKMARK)
  # First call to get the required number of bytes (not units!), including space for a NUL terminator
  n_bytes = pdfium_c.FPDFBookmark_GetTitle(bookmark, None, 0)
  # Initialise the output buffer
  buffer = ctypes.create_string_buffer(n_bytes)
  # Second call with the actual buffer
  pdfium_c.FPDFBookmark_GetTitle(bookmark, buffer, n_bytes)
  # Decode to string, cutting off the NUL terminator (encoding: UTF-16LE)
  title = buffer.raw[:n_bytes-2].decode("utf-16-le")
  ```
  
  Example B: Extracting text in given boundaries.
  ```python
  # (Assuming `textpage` is an FPDF_TEXTPAGE and the boundary variables are set)
  # Store common arguments for the two calls
  args = (textpage, left, top, right, bottom)
  # First call to get the required number of units (not bytes!) - a possible NUL terminator is not included
  n_chars = pdfium_c.FPDFText_GetBoundedText(*args, None, 0)
  # If no characters were found, return an empty string
  if n_chars <= 0:
      return ""
  # Calculate the required number of bytes (encoding: UTF-16LE again)
  # The function signature uses c_ushort, so 1 unit takes sizeof(c_ushort) == 2 bytes
  n_bytes = 2 * n_chars
  # Initialise the output buffer - this function can work without NUL terminator, so skip it
  buffer = ctypes.create_string_buffer(n_bytes)
  # Re-interpret the type from char to unsigned short* as required by the function
  buffer_ptr = ctypes.cast(buffer, ctypes.POINTER(ctypes.c_ushort))
  # Second call with the actual buffer
  pdfium_c.FPDFText_GetBoundedText(*args, buffer_ptr, n_chars)
  # Decode to string (You may want to pass `errors="ignore"` to skip possible errors in the PDF's encoding)
  text = buffer.raw.decode("utf-16-le")
  ```

* Not only are there different ways of string output that need to be handled according to the requirements of the function in question.
  String input, too, can work differently depending on encoding and type.
  We have already discussed `FPDF_LoadDocument()`, which takes a UTF-8 encoded string as `char*`.
  A different examples is `FPDFText_FindStart()`, which needs a UTF-16LE encoded string, given as `unsigned short*`:
  ```python
  # (Assuming `text` is a str and `textpage` an FPDF_TEXTPAGE)
  # Add the NUL terminator and encode as UTF-16LE
  enc_text = (text + "\x00").encode("utf-16-le")
  # cast `enc_text` to a c_ushort pointer
  text_ptr = ctypes.cast(enc_text, ctypes.POINTER(ctypes.c_ushort))
  search = pdfium_c.FPDFText_FindStart(textpage, text_ptr, 0, 0)
  ```

* Leaving strings, let's suppose you have a C memory buffer allocated by PDFium and wish to read its data.
  PDFium will provide you with a pointer to the first item of the byte array.
  To access the data, you'll want to re-interpret the pointer as an array view with `.from_address()`:
  ```python
  # (Assuming `bitmap` is an FPDF_BITMAP and `size` is the expected number of bytes in the buffer)
  # FPDFBitmap_GetBuffer() has c_void_p as restype, which ctypes will auto-resolve to int or None
  buffer_ptrval = pdfium_c.FPDFBitmap_GetBuffer(bitmap)
  assert buffer_ptrval  # make sure it's non-null
  # Get an actual pointer object so we can access .contents
  buffer_ptr = ctypes.cast(buffer_ptrval, ctypes.POINTER(ctypes.c_ubyte))
  # Buffer as ctypes array (referencing the original buffer, will be unavailable as soon as the bitmap is destroyed)
  c_buffer = (ctypes.c_ubyte * size).from_address( ctypes.addressof(buffer_ptr.contents) )
  # Buffer as Python bytes (independent copy)
  py_buffer = bytes(c_buffer)
  ```
  Note that you can achieve the same result with `ctypes.cast(ptr, POINTER(type * size)).contents`, but this is somewhat problematic since ctypes used to cache pointer types eternally with Python < 3.14 (as `size` may vary, this can lead to memory leak like scenarios with long-running applications).

* Writing data from Python into a C buffer works in a similar fashion:
  ```python
  # (Assuming `buffer_ptr` is a pointer to the first item of a C buffer to write into,
  #  `size` the number of bytes it can store, and `py_buffer` a Python byte buffer)
  buffer = (ctypes.c_ubyte * size).from_address( ctypes.addressof(buffer_ptr.contents) )
  # Read from the Python buffer, starting at its current position, directly into the C buffer
  # (until the target is full or the end of the source is reached)
  n_bytes = py_buffer.readinto(buffer)  # returns the number of bytes read
  ```

* If you wish to check whether two objects returned by PDFium are the same, the `is` operator won't help because `ctypes` does not have original object return (OOR), i.e. new, equivalent Python objects are created each time, although they might represent one and the same C object.[^ctypes_no_oor]
  That's why you'll want to use `ctypes.addressof()` to get the memory addresses of the underlying C object.
  For instance, this is used to avoid infinite loops on circular bookmark references when iterating through the document outline:
  ```python
  # (Assuming `pdf` is an FPDF_DOCUMENT)
  seen = set()
  bookmark = pdfium_c.FPDFBookmark_GetFirstChild(pdf, None)
  while bookmark:
      # bookmark is a pointer, so we need to use its `contents` attribute to get the object the pointer refers to
      # (otherwise we'd only get the memory address of the pointer itself, which would result in random behaviour)
      address = ctypes.addressof(bookmark.contents)
      if address in seen:
          break  # circular reference detected
      else:
          seen.add(address)
      bookmark = pdfium_c.FPDFBookmark_GetNextSibling(pdf, bookmark)
  ```
  
  [^ctypes_no_oor]: Confer the [ctypes documentation on Pointers](https://docs.python.org/3/library/ctypes.html#pointers).

* In many situations, callback functions come in handy.[^callback_usecases] Thanks to `ctypes`, it is seamlessly possible to use callbacks across Python/C language boundaries.
  
  [^callback_usecases]: e. g. incremental read/write, management of progressive tasks, ...
  
  Example: Loading a document from a Python buffer. This way, file access can be controlled in Python while the data does not need to be in memory at once.
  ```python
  import os
  
  # Factory class to create callable objects holding a reference to a Python buffer
  class _reader_class:
    
    def __init__(self, py_buffer):
        self.py_buffer = py_buffer
    
    def __call__(self, _, position, buffer_ptr, size):
        # Write data from Python buffer into C buffer, as explained before
        c_buffer = (ctypes.c_ubyte * size).from_address( ctypes.addressof(buffer_ptr.contents) )
        self.py_buffer.seek(position)
        self.py_buffer.readinto(c_buffer)
        return 1  # non-zero return code for success
  
  # (Assuming py_buffer is a Python file buffer, e. g. io.BufferedReader)
  # Get the length of the buffer
  py_buffer.seek(0, os.SEEK_END)
  file_len = py_buffer.tell()
  py_buffer.seek(0)
  
  # Set up an interface structure for custom file access
  fileaccess = pdfium_c.FPDF_FILEACCESS()
  fileaccess.m_FileLen = file_len
  
  # Assign the callback, wrapped in its CFUNCTYPE
  fileaccess.m_GetBlock = type(fileaccess.m_GetBlock)( _reader_class(py_buffer) )
  
  # Finally, load the document
  pdf = pdfium_c.FPDF_LoadCustomDocument(fileaccess, None)
  ```

<!-- TODO mention pdfium_i.get_bufreader() as a shortcut to set up an FPDF_FILEACCESS interface -->
<!-- FIXME The data holder strategy shown below is wonky. Should use Py_IncRef() / Py_DecRef() C APIs instead. -->

* When using the raw API, special care needs to be taken regarding object lifetime, considering that Python may garbage collect objects as soon as their reference count reaches zero. However, the interpreter has no way of magically knowing how long the underlying resources of a Python object might still be needed on the C side, so measures need to be taken to keep such objects referenced until PDFium does not depend on them anymore.
  
  If resources need to remain valid after the time of a function call, PDFium docs usually indicate this clearly. Ignoring requirements on object lifetime will lead to memory corruption (commonly resulting in a segfault sooner or later).
  
  For instance, the docs on `FPDF_LoadCustomDocument()` state that
  > The application must keep the file resources |pFileAccess| points to valid until the returned FPDF_DOCUMENT is closed. |pFileAccess| itself does not need to outlive the FPDF_DOCUMENT.
  
  This means that the callback function and the Python buffer need to be kept alive as long as the `FPDF_DOCUMENT` is used.
  This can be achieved by referencing these objects in an accompanying class, e. g.
  
  ```python
  class PdfDataHolder:
      
      def __init__(self, buffer, function):
          self.buffer = buffer
          self.function = function
      
      def close(self):
          # Make sure both objects remain available until this function is called
          # No-op id() call to denote that the object needs to stay in memory up to this point
          id(self.function)
          self.buffer.close()
  
  # ... set up an FPDF_FILEACCESS structure
  
  # (Assuming `py_buffer` is the buffer and `fileaccess` the FPDF_FILEACCESS interface)
  data_holder = PdfDataHolder(py_buffer, fileaccess.m_GetBlock)
  pdf = pdfium_c.FPDF_LoadCustomDocument(fileaccess, None)
  
  # ... work with the pdf
  
  # Close the PDF to free resources
  pdfium_c.FPDF_CloseDocument(pdf)
  # Close the data holder, to keep the object itself and thereby the objects it
  # references alive up to this point, as well as to release the buffer
  data_holder.close()
  ```

* Finally, let's finish this guide with an example how to render the first page of a document to a `PIL` image in `RGBA` color format.
  ```python
  import math
  import ctypes
  import os.path
  import PIL.Image
  import pypdfium2.raw as pdfium_c
  
  # Load the document
  filepath = os.path.abspath("tests/resources/render.pdf")
  pdf = pdfium_c.FPDF_LoadDocument((filepath+"\x00").encode("utf-8"), None)
  
  # Check page count to make sure it was loaded correctly
  page_count = pdfium_c.FPDF_GetPageCount(pdf)
  assert page_count >= 1
  
  # Load the first page and get its dimensions
  page = pdfium_c.FPDF_LoadPage(pdf, 0)
  width  = math.ceil(pdfium_c.FPDF_GetPageWidthF(page))
  height = math.ceil(pdfium_c.FPDF_GetPageHeightF(page))
  
  # Create a bitmap
  # (Note, pdfium is faster at rendering transparency if we use BGRA rather than BGRx)
  use_alpha = pdfium_c.FPDFPage_HasTransparency(page)
  bitmap = pdfium_c.FPDFBitmap_Create(width, height, int(use_alpha))
  # Fill the whole bitmap with a white background
  # The color is given as a 32-bit integer in ARGB format (8 bits per channel)
  pdfium_c.FPDFBitmap_FillRect(bitmap, 0, 0, width, height, 0xFFFFFFFF)
  
  # Store common rendering arguments
  render_args = (
      bitmap,  # the bitmap
      page,    # the page
      # positions and sizes are to be given in pixels and may exceed the bitmap
      0,       # left start position
      0,       # top start position
      width,   # horizontal size
      height,  # vertical size
      0,       # rotation (as constant, not in degrees!)
      pdfium_c.FPDF_LCD_TEXT | pdfium_c.FPDF_ANNOT,  # rendering flags, combined with binary or
  )
  
  # Render the page
  pdfium_c.FPDF_RenderPageBitmap(*render_args)
  
  # Get the value of a pointer to the first item of the buffer
  buffer_ptrval = pdfium_c.FPDFBitmap_GetBuffer(bitmap)
  assert buffer_ptrval, "buffer pointer value must be non-null"
  # Cast the pointer value to an actual pointer object so we can access .contents
  buffer_ptr = ctypes.cast(buffer_ptrval, ctypes.POINTER(ctypes.c_ubyte))
  # Re-interpret as array
  buffer = (ctypes.c_ubyte * (width * height * 4)).from_address(ctypes.addressof(buffer_ptr.contents))
  
  # Create a PIL image from the buffer contents
  img = PIL.Image.frombuffer("RGBA", (width, height), buffer, "raw", "BGRA", 0, 1)
  # Save it as file
  img.save("out.png")
  
  # Free resources
  pdfium_c.FPDFBitmap_Destroy(bitmap)
  pdfium_c.FPDF_ClosePage(page)
  pdfium_c.FPDF_CloseDocument(pdf)
  ```

### [Command-line Interface](https://pypdfium2.readthedocs.io/en/stable/shell_api.html)

pypdfium2 also ships with a simple command-line interface, providing access to key features of the support model in a shell environment (e. g. rendering, content extraction, document inspection, page rearranging, ...).

The primary motivation behind this is to have a nice testing interface, but it may be helpful in a variety of other situations as well.
Usage should be largely self-explanatory, assuming some familiarity with the command-line.
See `pypdfium2 --help` or `pypdfium2 $SUBCOMMAND --help` for available commands and options.


## Licensing

> [!NOTE]
> *Disclaimer: This project is provided on an "as-is" basis. This is not legal advice, and there is ABSOLUTELY NO WARRANTY for any information provided in this document or elsewhere in the pypdfium2 project, including earlier revisions.*
> *We disclaim liability for any possible damages resulting from using this license information. It is the embedder's responsibility to check on licensing.*
> *See also [GitHub's disclaimer](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/licensing-a-repository#disclaimer).*

pypdfium2 itself is available by the terms and conditions of [`Apache-2.0`](LICENSES/Apache-2.0.txt) / [`BSD-3-Clause`](LICENSES/BSD-3-Clause.txt).
Documentation and examples of pypdfium2 are licensed under [`CC-BY-4.0`](LICENSES/CC-BY-4.0.txt).
pypdfium2 includes [SPDX](https://spdx.org/licenses/) headers in source files.
License information for data files is provided in [`REUSE.toml`](REUSE.toml) as per the [`reuse` standard](https://reuse.software/spec/).

PDFium is available under "a BSD-style license that can be found in \[its\] [`LICENSE`](https://pdfium.googlesource.com/pdfium/+/refs/heads/main/LICENSE) file".<br>
Various other open-source licenses apply to dependencies included with PDFium.
PDFium's license as well as dependency licenses have to be shipped with binary distributions.<br>
See the [`BUILD_LICENSES/`](BUILD_LICENSES/) directory, or the licenses shipped with our wheel builds.

PDFium's dependencies might change over time. Please notify us if you think a relevant license is missing.

To the author's knowledge, pypdfium2 is one of the rare Python libraries capable of PDF rendering while not being covered by strong-copyleft licenses.[^liberal_pdf_renderlibs]

> [!IMPORTANT]
> The exact licensing situation depends on how the builds were made.<br>
> Note that a subset of pypdfium2 builds might link with the `libgcc` runtime library. Check the builds you use and, if affected, libgcc's license to see if that's OK for your use.

[^liberal_pdf_renderlibs]: The only other liberal-licensed PDF rendering libraries known to the author are [`pdf.js`](https://github.com/mozilla/pdf.js/) (JavaScript) and [`Apache PDFBox`](https://github.com/apache/pdfbox) (Java), but python bindings packages don't exist yet or are unsatisfactory. However, we wrote some gists that show it'd be possible in principle: [pdfbox](https://gist.github.com/mara004/51c3216a9eabd3dcbc78a86d877a61dc) (+ [setup](https://gist.github.com/mara004/881d0c5a99b8444fd5d1d21a333b70f8)), [pdfjs](https://gist.github.com/mara004/87276da4f8be31c80c38036c6ab667d7).


## Issues / Contributions

While using pypdfium2, you might encounter bugs or missing features.
In this case, feel free to open an issue or discussion thread. If applicable, include details such as tracebacks, OS and CPU type, as well as the versions of pypdfium2 and used dependencies.

Roadmap:
* pypdfium2
  - [Issues panel](https://github.com/pypdfium2-team/pypdfium2/issues): Initial bug reports and feature requests. May need to be transferred to dependencies.
  - [Discussions page](https://github.com/pypdfium2-team/pypdfium2/discussions): General questions and suggestions.
* PDFium
  - [Bug tracker](https://crbug.com/pdfium/): Issues in PDFium.
    Beware: The bridge between Python and C increases the probability of integration issues or API misuse. The symptoms can often make it look like a PDFium bug while it is not.
  - [Mailing list](https://groups.google.com/g/pdfium/): Questions regarding PDFium usage.
* [pdfium-binaries](https://github.com/bblanchon/pdfium-binaries/issues): Binary builder.
* [ctypesgen](https://github.com/pypdfium2-team/ctypesgen/issues): Bindings generator (fork). See also [upstream](https://github.com/ctypesgen/ctypesgen/issues).

### Response policy
<!-- Inspired by bluesky's contribution rules: https://github.com/bluesky-social/indigo -->

Given this is a volunteer open-source project, it is possible you may not get a response to your issue, or it may be closed without much feedback. Conversations may be locked if we feel like our attention is getting DDOSed. We may not have time to provide usage support.

The same applies to Pull Requests. We will accept contributions only if we find them suitable. Do not reach out with a strong expectation to get your change merged; it is solely up to the repository owner to decide if and when a PR will be merged, and we are free to silently reject PRs we do not like.

### Known limitations

#### Incompatibility with Threading

PDFium is inherently not thread-safe. See the [API docs](https://pypdfium2.readthedocs.io/en/stable/python_api.html#incompatibility-with-threading) for more information.

#### Risk of unknown object lifetime violations

As outlined in the raw API section, it is essential that Python-managed resources remain available as long as they are needed by PDFium.

The problem is that the Python interpreter may garbage collect objects with reference count zero at any time, so it can happen that an unreferenced but still required object by chance stays around long enough before it is garbage collected. However, it could also disappear too soon and cause breakage. Such dangling objects result in non-deterministic memory issues that are hard to debug.
If the timeframe between reaching reference count zero and removal is sufficiently large and roughly consistent across different runs, it is even possible that mistakes regarding object lifetime remain unnoticed for a long time.

Although we intend to develop helpers carefully, it cannot be fully excluded that unknown object lifetime violations might still be lurking around somewhere, especially if unexpected requirements were not documented by the time the code was written.

#### Missing raw PDF access

As of this writing, PDFium's public interface does not provide access to the raw PDF data structure (see [issue 1694](https://crbug.com/pdfium/1694)). It does not expose APIs to read/write PDF dictionaries, streams, name/number trees, etc. Instead, it merely offers a predefined set of abstracted functions. This considerably limits the library's potential, compared to other products such as `pikepdf`.

#### Limitations of ABI bindings

PDFium's non-public backend would provide extended capabilities, including [raw access](#missing-raw-pdf-access), but it is written in C++, which (unlike pure C) does not result in a stable ABI, so we cannot use it with `ctypes`. This means it's out of scope for this project.

Also, while ABI bindings tend to be more convenient, they have some technical drawbacks compared to API bindings (see e.g. [1](https://cffi.readthedocs.io/en/latest/overview.html#abi-versus-api), [2](https://github.com/ocrmypdf/OCRmyPDF/issues/541#issuecomment-1834684532))


## Development
<!-- TODO wheel tags, maintainer access, GitHub peculiarities -->

### Long lines

The pypdfium2 codebase does not hard wrap long lines.
It is recommended to set up automatic word wrap in your text editor, e.g. VS Code:
```
editor.wordWrap = bounded
editor.wordWrapColumn = 100
```

### Command recipes

The pypdfium2 project uses the [`just` command runner](https://github.com/casey/just), which can be seen as a more modern, more flexible alternative to `make`. In particular, there's no good way to pass through positional arguments with `make`.

Run `just -l` (or open the `justfile`) to view the available commands.

### Docs

pypdfium2 provides API documentation using [Sphinx](https://github.com/sphinx-doc/sphinx/), which can be rendered to various formats, including HTML:
```bash
sphinx-build -b html ./docs/source ./docs/build/html/
just docs-build  # short alias
```
<!-- there's also a Makefile in docs/ as generated by sphinx-quickstart (cd docs/; make build) -->

Built docs are primarily hosted on [`readthedocs.org`](https://readthedocs.org/projects/pypdfium2/).
It may be configured using a [`.readthedocs.yaml`](.readthedocs.yaml) file (see [instructions](https://docs.readthedocs.io/en/stable/config-file/v2.html)), and the administration page on the web interface.
RTD theoretically supports hosting multiple versions, but currently, we only host one build for the latest release through the `stable` branch.
New builds are automatically triggered by a webhook whenever a linked branch is pushed.

Additionally, one doc build can also be hosted on [GitHub Pages](https://pypdfium2-team.github.io/pypdfium2/index.html).
It is implemented with a CI workflow, which is supposed to be triggered automatically on release.
This provides us with full control over build env and used commands, whereas RTD may be less liberal in this regard.

### Testing

pypdfium2 contains a small test suite to verify the library's functionality. It is written with [pytest](https://github.com/pytest-dev/pytest/):
```bash
python -m pytest tests/  # or `just test`
```

Note that ...
* you can pass `-sv` to get more detailed output.
* `$DEBUG_AUTOCLOSE=1` may be set to get debugging information on automatic object finalization.

To get code coverage statistics, you may call
```bash
just coverage
```

<!-- TODO any chance to avoid `bash -c` ? -->

Sometimes, it can also be helpful to test code on many PDFs.[^testing_corpora]
In this case, the command-line interface and `find` come in handy:
```bash
# Example A: Analyse PDF images (in the current working directory)
find . -name '*.pdf' -exec bash -c "echo \"{}\" && pypdfium2 pageobjects \"{}\" --filter image" \;
# Example B: Parse PDF table of contents
find . -name '*.pdf' -exec bash -c "echo \"{}\" && pypdfium2 toc \"{}\"" \;
```

[^testing_corpora]: For instance, one could use the testing corpora of open-source PDF libraries (pdfium, pikepdf/ocrmypdf, mupdf/ghostscript, tika/pdfbox, pdfjs, ...)

### Release workflow

The release process is fully automated using Python scripts and scheduled release workflows.
You may also trigger the workflow manually from the GitHub Actions panel or similar.

Python release scripts are located in the folder `setupsrc`, along with custom setup code:
* `update.py` downloads binaries.
* `craft.py` builds platform-specific wheel packages and a source distribution suitable for PyPI upload.
* `autorelease.py` takes care of versioning, changelog, release note generation and VCS check-in.

The autorelease script has some peculiarities maintainers should know about:
* The changelog for the next release shall be written into `docs/devel/changelog_staging.md`.
  On release, it will be moved into the main changelog under `docs/source/changelog.md`, annotated with the PDFium version update.
  It will also be shown on the GitHub release page.
* pypdfium2 versioning uses the pattern `major.minor.patch`, optionally with an appended beta mark (e. g. `2.7.1`, `2.11.0`, `3.0.0b1`, ...).
  Update types such as major or beta may be controlled via `autorelease/config.json`

In case of necessity, you may also forego CI and do the release locally, which would roughly work like this (though ideally it should never be needed):
* Run the autorelease script (this will implicitly switch onto the `autorelease_tmp` branch, make a release commit, and create the tag):
  ```bash
  python3 setupsrc/autorelease.py --register
  ```
* Merge the `autorelease_tmp` branch:
  ```bash
  git checkout main
  git merge autorelease_tmp
  ```
* Build the packages
  ```bash
  just packaging-pypi
  ```
* Push release commit/tag to the repository:
  ```bash
  git push
  git push --tags
  ```
* Upload to PyPI
  ```bash
  # this will interactively ask for your username/password
  twine upload dist/*
  ```
* Update the `stable` branch to trigger a documentation rebuild
  ```bash
  git checkout stable
  git rebase origin/main  # or: git reset --hard main
  git push
  git checkout main
  ```

If something went wrong with commit or tag, you can still revert the changes:
```bash
# perform an interactive rebase to change history (substitute $N_COMMITS with the number of commits to drop or modify)
git rebase -i HEAD~$N_COMMITS
git push --force
# delete remote tag (substitute $TAGNAME accordingly)
git push --delete origin $TAGNAME
# delete local tag
git tag -d $TAGNAME
```
Faulty PyPI releases may be yanked using the web interface.


## Popular dependents

pypdfium2 is used by popular packages such as
[langchain](https://github.com/langchain-ai/langchain),
[dify](https://github.com/langgenius/dify),
[docling](https://github.com/DS4SD/docling),
[nougat](https://github.com/facebookresearch/nougat),
[pdfplumber](https://github.com/jsvine/pdfplumber),
[doctr](https://github.com/mindee/doctr/),
and [nv-ingest](https://github.com/NVIDIA/nv-ingest).

This results in pypdfium2 being part of a large dependency tree.


## Thanks to[^thanks_to]

* [Benoît Blanchon](https://github.com/bblanchon): Author/Maintainer of [PDFium binaries](https://github.com/bblanchon/pdfium-binaries/) and [patches](https://github.com/bblanchon/pdfium-binaries/tree/master/patches).
* [Yinlin Hu](https://github.com/YinlinHu): `pypdfium` prototype and `kuafu` PDF viewer.
* [Mike Kroutikov](https://github.com/mkroutikov): Examples on how to use PDFium in `redstork`, `redstork-ui` and `pdfbrain`.
* [Tim Head](https://github.com/betatim): Original idea for Python bindings to PDFium with ctypesgen in `wowpng`.
* [Adam Huganir](https://github.com/adam-huganir): Help with maintenance and development decisions since the beginning of the project.
* [Christian Heimes](https://github.com/tiran): RPM packaging for pdfium. Showing how to build pdfium natively without Google's toolchain.
* [Marvin Gießing](https://github.com/mgiessing): Investigation on building PDFium for a then unhandled Linux architecture (ppc64le).
* [wojiushixiaobai](https://github.com/wojiushixiaobai): Helpful pointers and draft workflow for cibuildwheel. Supporting exotic architectures via emulation (pulling in dependencies with auditwheel).
* [kobaltcore](https://github.com/kobaltcore): Bug fix for `PdfDocument.save()`.
* [Anderson Bravalheri](https://github.com/abravalheri): Help with PEP 517/518 compliance. Hint to use an environment variable rather than separate setup files.
* [Bastian Germann](https://github.com/bgermann): Help with inclusion of licenses for third-party components of PDFium.

... and further [code contributors](https://github.com/pypdfium2-team/pypdfium2/graphs/contributors) (GitHub stats).

*If you have contributed to this project but are not mentioned here yet, please let us know.*

[^thanks_to]: People listed in this section may not necessarily have contributed any copyrightable code to the repository. Many have rather helped with ideas, or contributions to dependencies of pypdfium2.


## History

### PDFium

The PDFium code base was originally developed as part of the commercial Foxit SDK, before being acquired and open-sourced by Google, who maintain PDFium independently ever since, while Foxit continue to develop their SDK closed-source.

### pypdfium2

pypdfium2 is the successor of *pypdfium* and *pypdfium-reboot*.

Inspired by *wowpng*, the first known proof of concept Python binding to PDFium using ctypesgen, the initial *pypdfium* package was created. It had to be updated manually, which did not happen frequently. There were no platform-specific wheels, but only a single wheel that contained binaries for 64-bit Linux, Windows and macOS.

*pypdfium-reboot* then added a script to automate binary deployment and bindings generation to simplify regular updates. However, it was still not platform specific.

pypdfium2 is a full rewrite of *pypdfium-reboot* to build platform-specific wheels and consolidate the setup scripts. Further additions include ...
* A CI workflow to automatically release new wheels at a defined schedule
* Convenience support models that wrap the raw PDFium/ctypes API
* Test code
* A script to build PDFium from source

Chun-wei Fan `<fanc999@yahoo.com.tw>`
Philip Withnall `<withnall@endlessm.com>`
Nirbheek Chauhan `<nirbheek@centricular.com>`

This document was last updated in 2019. You're reading this in the future, and
lots of information might be misleading or outdated in your age. You have been
warned.

# General

For prebuilt binaries (DLLs and EXEs) and developer packages (headers,
import libraries) of GLib, Pango, GTK+ etc for Windows, go to
https://www.gtk.org/download/windows.php . They are for "native"
Windows meaning they use the Win32 API and Microsoft C runtime library
only. No POSIX (Unix) emulation layer like Cygwin is involved.

To build GLib on Win32, you can use either GCC ("MinGW") or the Microsoft
Visual Studio toolchain. For the latter, Visual Studio 2015 and later are
recommended. For older Visual Studio versions, see below.

You can also cross-compile GLib for Windows from Linux using the
cross-compiling mingw packages for your distro.

Note that to just *use* GLib on Windows, there is no need to build it
yourself.

On Windows setting up a correct build environment is very similar to typing
`meson; ninja` like on Linux.

The following preprocessor macros are to be used for conditional
compilation related to Win32 in GLib-using code:

- `G_OS_WIN32` is defined when compiling for native Win32, without
  any POSIX emulation, other than to the extent provided by the
  bundled Microsoft C library.

- `G_WITH_CYGWIN` is defined if compiling for the Cygwin
  environment. Note that `G_OS_WIN32` is *not* defined in that case, as
  Cygwin is supposed to behave like Unix. `G_OS_UNIX` *is* defined by a GLib
  for Cygwin.

- `G_PLATFORM_WIN32` is defined when either `G_OS_WIN32` or `G_WITH_CYGWIN`
  is defined.

These macros are defined in `glibconfig.h`, and are thus available in
all source files that include `<glib.h>`.

Additionally, there are the compiler-specific macros:
- `__GNUC__` is defined when using GCC or Clang
- `__clang__` is defined when using Clang or Clang-CL
- `_MSC_VER` is defined when using MSVC or Clang-CL

`G_OS_WIN32` implies using the Microsoft C runtime, which used to be
`msvcrt.dll` and is now the [Universal CRT](https://docs.microsoft.com/en-us/cpp/c-runtime-library/crt-library-features?view=vs-2015)
when building with Visual Studio. When using the MinGW-GCC toolchain, the CRT
in use depends on the settings used while the toolchain was built. We highly
recommend [using the Universal CRT when building with
MinGW](https://mingwpy.github.io/ucrt.html) too.

GLib is not actively tested with the static versions of the UCRT, but if you
need to use those, patches are welcome.

# Building software that use GLib or GTK+

Building software that just *uses* GLib or GTK+ also require to have
the right compiler set up the right way. If you intend to use MinGW-GCC,
follow the relevant instructions below in that case, too.

You should link to GLib using the `-mms-bitfields` GCC flag. This flag means
that the struct layout rules are identical to those used by MSVC. This is
essential if the same DLLs are to be usable both from gcc- and MSVC-compiled
code.

## Cross-CRT issues

You should take care that the DLLs that your code links to are using the same
C runtime library. Not doing so can and likely will lead to panics and crashes
**unless** you're very careful while passing objects allocated by a library
linked with one CRT to a library linked to another CRT, or (more commonly) not
doing that at all.

If you *do* pass CRT objects across CRT boundaries, do not file any issues
about whatever happens next.

To give an example, opening a `FILE` handle created by one CRT cannot be
understood by any other CRT, and will lead to an access violation. You also
cannot allocate memory in one CRT and free it using another.

There are [many other cases where you must not allow objects to cross CRT boundaries](https://docs.microsoft.com/en-us/cpp/c-runtime-library/potential-errors-passing-crt-objects-across-dll-boundaries?view=vs-2019),
but in theory if you're **very very** careful, you can make things work. Again,
please do not come to us for help if you choose to do this.

# Building GLib

You can build GLib with MinGW-GCC, MSVC, or (experimentally) with Clang-CL.

For all compilers, you will need the following:

- Install Python 3.6.x or newer, either 32-bit or 64-bit. We recommend enabling
  the option to add it to your `PATH`.
- [Install Meson](https://mesonbuild.com/Getting-meson.html)
- Install the [Ninja build tool](https://github.com/ninja-build/ninja/releases), which can also be
  installed with `pip3`. You can skip this step if you want to generate Visual
  Studio project files.
- [git for Windows](https://gitforwindows.org/) is required, since Meson makes
  use of git to download dependencies using subprojects.

## Building with MinGW-GCC

Open your MSYS or [MSYS2](https://www.msys2.org/) shell where you have the
MinGW-GCC toolchain installed, and build GLib [like any other Meson
project](https://mesonbuild.com/Quick-guide.html#compiling-a-meson-project).

## Building with Visual Studio 2015 or newer

Meson is now the only supported method of building GLib using Visual Studio.

To do a build using Meson, do the following:

- Open a Visual Studio (or SDK) command prompt that matches the Visual Studio
  version and build platform (Win32/x86, x64, etc.) that will be used in all
  the following steps.

- Create an empty directory/folder for the build inside your GLib sources
  directory, say, `_builddir`, and `cd` into it.

- Set up the build using Meson:

```cmd
> meson .. --buildtype=<release|debug|debugoptimized> --prefix=<path> [--backend=vs]
```

 Please see [the Meson docs](https://mesonbuild.com/Builtin-options.html#core-options)
 for an explanation for `--buildtype`.

 The path passed for `--prefix` need not to be on the same drive as where the
 build is carried out, but it is recommended to use forward slashes for this
 path.  The `--backend=vs` option can be used if the Visual Studio project
 generator is preferred over using Ninja.

- Build, test and install the build:
  Run `ninja` to build, `meson test` to test and `meson install` to install the
  build. If you used `--backend=vs`, instead of running `ninja`, you need to
  use `msbuild` or you can open the generated solution in Visual Studio.

## Building with old versions of Visual Studio

The steps are the same as above, with the following notes about issues that you might face.

### C4819 build errors

If you are building GLib-based libraries or applications, or GLib itself
and you see a `C4819` error (or warning, before `C4819` is treated as an error
in `msvc_recommended_pragmas.h`), please be advised that this error/warning should
not be disregarded, as this likely means portions of the build are not being
done correctly, as this is an issue of Visual Studio running on CJK (East Asian)
locales.  This is an issue that also affects builds of other projects, such as
QT, Firefox, LibreOffice/OpenOffice, Pango and GTK, along with many other projects.

To overcome this problem, please set your system's locale setting for non-Unicode to
English (United States), reboot, and restart the build, and the code should build
normally.

### Support for pre-2012 Visual Studio

This release of GLib requires at least the Windows 8 SDK in order to be built
successfully using Visual Studio, which means that it is no longer supported to
build GLib with Visual Studio 2008 nor 2010.  People that still need to use
Visual Studio 2008 or 2010 should continue to use glib-2.66.x.

project('librsvg',
        'c',
        version: '2.62.0', # Keep this in sync with Cargo.toml, doc/librsvg.toml, rsvg/Cargo.toml
        meson_version: '>= 1.3.0',
        default_options: [
        ]
)

librsvg_api_version = '2.0'
api_split = librsvg_api_version.split('.')
librsvg_api_major = api_split[0]
librsvg_api_minor = api_split[1]

librsvg_pc = 'librsvg-@0@'.format(librsvg_api_version)

host_system = host_machine.system()
cc = meson.get_compiler('c')

# MSRV - Minimum Supported Rust Version
# If you change this, please update these:
#   - the "rust-version" value in Cargo.toml
#   - the "Compilers and build tools" section of devel-docs/_build_dependencies.rst
#   - the "RUST_MINIMUM" variable in ci/container_builds.yml
msrv = '1.92.0'
cargo_cbuild_version = '0.10.10'
cargo = find_program('cargo', version:'>= @0@'.format(msrv))
cargo_wrapper = find_program('meson/cargo_wrapper.py', native: true)
cargo_c = find_program('cargo-cbuild', version:'>= @0@'.format(cargo_cbuild_version)) # https://github.com/lu-zero/cargo-c
rustc = find_program('rustc', version:'>= @0@'.format(msrv))
makedef = find_program('meson/makedef.py', native: true)

if host_system in ['darwin', 'ios']
  # Validate unconditional presence of getentropy and CCRandomGenerateBytes.
  # https://github.com/rust-lang/rust/pull/116319
  ret = cc.compiles('''#include <Availability.h>
#include <TargetConditionals.h>
#if !((TARGET_OS_OSX && __MAC_OS_X_VERSION_MIN_REQUIRED >= 101200L) || (TARGET_OS_IOS && __IPHONE_OS_VERSION_MIN_REQUIRED >= 100000L))
# error "https://gitlab.gnome.org/GNOME/librsvg/-/issues/1097"
#endif''',
    name: 'Targets at least macOS 10.12 or iOS 10',
  )
  if rustc.version().version_compare('>= 1.75.0') and not ret
    error('When using rustc >=1.75, you must target at least macOS 10.12 or iOS 10')
  endif
endif

py = import('python')

python = py.find_installation()

# Required versions for dependencies - Please update _build_dependencies.rst with version numbers if these change
cairo_required         = '>= 1.18.0'
dav1d_required         = '>= 1.3.0'
ft2_cmake_required     = '>= 2.8.0'   # Actual FreeType version required
freetype2_required     = '>= 20.0.14' # Corresponds to ft2_cmake_required
glib_required          = '>= 2.50.0'
harfbuzz_required      = '>= 2.0.0'
libxml_required        = '>= 2.9.0'
pango_required         = '>= 1.50.0'

# Optional dependencies
gdk_pixbuf_required    = '>= 2.20'
gidocgen_required      = '>= 2021.1'
introspection_required = '>= 1.39.0'
vapigen_required       = '>= 0.17.1.26'
# FIXME: add subprojects for the deps above

cairo_dep      = dependency('cairo',
                            version: cairo_required,
                            fallback: ['cairo', 'libcairo_dep'])
cairogobj_dep  = dependency('cairo-gobject',
                            version: cairo_required,
                            fallback: ['cairo', 'libcairogobject_dep'])
cairo_png_dep  = dependency('cairo-png',
                            version: cairo_required,
                            fallback: ['cairo']) # FIXME: cairo has no _dep for this one
dav1d_dep      = dependency('dav1d',
                            version: dav1d_required,
                            method: 'pkg-config',
                            required: get_option('avif'))
freetype2_dep  = dependency('freetype2',
                            version: freetype2_required,
                            method: 'pkg-config',
                            required: false)

# Try again using CMake, as Windows builds of FreeType might not
# come with pkg-config files; sadly, pkg-config and CMake use
# different version schemes for FreeType
if not freetype2_dep.found()
  freetype2_dep  = dependency('FreeType',
                              version: ft2_cmake_required,
                              method: 'cmake',
                              fallback: ['freetype2', 'freetype_dep'])
endif
pixbuf_dep     = dependency('gdk-pixbuf-2.0',
                            version: gdk_pixbuf_required,
                            required: get_option('pixbuf'),
                            fallback: ['gdk-pixbuf', 'gdkpixbuf_dep'],
                            default_options: ['png=enabled', 'jpeg=enabled', 'builtin_loaders=png,jpeg', 'man=false'])
gio_dep        = dependency('gio-2.0',
                            version: glib_required,
                            fallback: ['glib', 'libgio_dep'])
glib_dep       = dependency('glib-2.0',
                            version: glib_required,
                            fallback: ['glib', 'libglib_dep'])
harfbuzz_dep   = dependency('harfbuzz',
                            version: harfbuzz_required,
                            fallback: ['harfbuzz', 'libharfbuzz_dep'])
libxml_dep     = dependency(['libxml-2.0', 'LibXml2'],
                            version: libxml_required)
pango_dep      = dependency('pango',
                            version: pango_required,
                            fallback: ['pango', 'libpango_dep'])
pangocairo_dep = dependency('pangocairo',
                            version: pango_required,
                            fallback: ['pango', 'libpangocairo_dep'])
pangoft2_dep   = dependency('pangoft2',
                            version: pango_required,
                            required: host_system not in ['windows', 'darwin', 'ios'],
                            fallback: ['pango', 'libpangoft2_dep'])
gmodule_dep    = dependency('gmodule-2.0', version: glib_required,
                            fallback : ['glib', 'libgmodule_dep'])
rst2man        = find_program('rst2man', 'rst2man.py',
                              required: get_option('docs'),
                              disabler: true)
gidocgen       = find_program('gi-docgen', version: gidocgen_required,
                            required: false,
                            disabler: true)
gi_dep         = dependency('gobject-introspection-1.0',
                            version: introspection_required,
                            required: get_option('introspection'))
vapigen_dep    = dependency('vapigen',
                            version: vapigen_required,
                            required: get_option('vala'))

# Extra env to pass to cargo
extra_env = environment()

# If FreeType and/or libxml2 is/are found by CMake instead of
# pkg-config, we must tell Cargo the libraries explicitly and
# disable pkg-config for these deps
if freetype2_dep.type_name() == 'cmake' or libxml_dep.type_name() == 'cmake'
  fs = import('fs')
endif
if freetype2_dep.type_name() == 'cmake'
  ft2_lib = freetype2_dep.get_variable('FREETYPE_LIBRARIES')
  extra_env.set('SYSTEM_DEPS_FREETYPE2_NO_PKG_CONFIG', '1')
  extra_env.set('SYSTEM_DEPS_FREETYPE2_SEARCH_NATIVE', fs.parent(ft2_lib))
  extra_env.set('SYSTEM_DEPS_FREETYPE2_LIB', fs.stem(ft2_lib))
endif
if libxml_dep.type_name() == 'cmake'
  libxml_lib = libxml_dep.get_variable('LIBXML2_LIBRARIES')
  extra_env.set('SYSTEM_DEPS_LIBXML2_NO_PKG_CONFIG', '1')
  extra_env.set('SYSTEM_DEPS_LIBXML2_SEARCH_NATIVE', fs.parent(libxml_lib))
  extra_env.set('SYSTEM_DEPS_LIBXML2_LIB', fs.stem(libxml_lib))
endif

if host_system == 'windows'
  build_gir = get_option('introspection').require(get_option('default_library') != 'static' and meson.can_run_host_binaries() and gi_dep.found())
else
  build_gir = get_option('introspection').require(meson.can_run_host_binaries() and gi_dep.found())
endif
build_vala = get_option('vala').require(meson.can_run_host_binaries() and vapigen_dep.found())

build_rsvg_convert = get_option('rsvg-convert')

build_pixbuf_loader = get_option('pixbuf-loader').require(pixbuf_dep.found())

build_docs = get_option('docs')

build_tests = get_option('tests')

gnome = import('gnome')

foundation_dep = dependency('appleframeworks', modules: 'Foundation',
                            required: host_system in ['darwin', 'ios'])
m_dep = cc.find_library('m', required: false)

# Solaris and Illumos distros split a lot of networking-related code
# into '-lsocket' and '-lnsl'.  Anything that calls socketpair(), getifaddr(),
# etc. probably needs to include network_deps
#
if host_system == 'sunos'
  network_deps = [
    cc.find_library('socket', required: false),
  ]
else
  network_deps = []
endif

# Use this in place of libxml2/freetype2 for 'Requires:' in the
# pkg-config file in case either or both were found with CMake
empty_pc_dep = dependency('', required: false)

# ignore the system dependencies for GIR
library_dependencies_sole = [
  glib_dep,
  gio_dep,
  pixbuf_dep,
  cairo_dep,
]
private_libraries = []
# these dependencies are not exported in the ABI
private_dependencies = [
  cairogobj_dep,
  cairo_png_dep,
  dav1d_dep,
  freetype2_dep.type_name() != 'cmake' ? freetype2_dep : empty_pc_dep,
  harfbuzz_dep,
  libxml_dep.type_name() != 'cmake' ? libxml_dep : empty_pc_dep,
  pangocairo_dep,
  pangoft2_dep,
  gmodule_dep,
]
library_dependencies = library_dependencies_sole
other_library_dependencies = []

foreach d: [libxml_dep, freetype2_dep]
  if d.type_name() == 'cmake'
    other_library_dependencies += d
  endif
endforeach

target = get_option('triplet')
target_arg = []

toolchain_arg = []
toolchain_ver = get_option('rustc-version')

check_version = false
# Check toolchain_ver if supplied, if in the form of a version
# We assume to continue if a stable, nightly or beta or custom toolchain is specified
if toolchain_ver != ''
  if host_system == 'windows'
    warning('\'rustc-version\' option currently only supported for Windows')
  endif
  check_version_script = find_program('meson/check-is-version-string.py', native: true)
  is_version_string_check = run_command(
    [check_version_script, '--string', toolchain_ver],
    capture: true,
    check: true
  )
  check_version = is_version_string_check.stdout().split()[0] == 'check'
  if check_version
    if not toolchain_ver.version_compare('>=@0@'.format(msrv))
      error('Specified Rust toolchain version @0@ is less than @1@'.format(toolchain_ver, msrv))
    endif
  endif
endif

# Ideally, cc.get_argument_syntax() == 'msvc' should
# cover this, but 'clang' can mean GCC or MSVC-style
# depending on the env and/or flags on Windows
is_msvc_style = cc.get_argument_syntax() == 'msvc' or cc.get_define('_MSC_VER') != ''

if target != ''
  target_arg += ['--target', target]
endif

query_rustc_harness = find_program('meson/query-rustc.py', native: true, required: get_option('default_library') != 'shared')

rustc_query_args = [
  query_rustc_harness,
  rustc,
]

if host_system == 'windows'
  # First deduce the target that is to be used for the build, based on Meson build settings
  host_cpu_family = host_machine.cpu_family()
  rustc_toolchain_cpu = host_cpu_family == 'x86' ? 'i686' : host_cpu_family
  if is_msvc_style
    rustc_toolchain_cc = 'msvc'
  elif cc.get_id() == 'clang'
    rustc_toolchain_cc = 'gnullvm'
  else
    rustc_toolchain_cc = 'gnu'
  endif
  rustc_target = '@0@-pc-windows-@1@'.format(rustc_toolchain_cpu, rustc_toolchain_cc)

  if toolchain_ver != ''
    build_triplet_arg = []
    if target_arg == []
      target_arg += ['--target', rustc_target]
    endif

    # If doing a cross build, with '-Drustc-version=...' we must find the native toolchain on the build
    # machine with its arch triplet that is to be used for building against the target, as Cargo will
    # require this in its command line
    if meson.is_cross_build()
      build_cpu_family = build_machine.cpu_family()
      rustc_build_toolchain_cpu = build_cpu_family == 'x86' ? 'i686' : build_cpu_family
      rustc_build_triplet = '@0@-pc-windows-@1@'.format(rustc_build_toolchain_cpu, rustc_toolchain_cc)
      build_triplet_arg += ['--build-triplet', rustc_build_triplet]
    endif

    # If a version number is used, also check against the actual release that is depicted
    # by the currently installed 'stable' toolchain, and use the stable toolchain if the requested
    # version matches the installed 'stable' toolchain
    if check_version
      stable_toolchain_arg = ['--toolchain-version', 'stable']

      stable_actual_version = run_command(
        rustc_query_args + ['--query=stable-actual-version'] + stable_toolchain_arg + target_arg + build_triplet_arg,
        capture: true,
        check: true
      )
      if stable_actual_version.stderr() != ''
        error('error occurred when querying stable toolchain: @0@'.format(default_host.stderr().split()[0]))
      endif
      stable_version = stable_actual_version.stdout().split()[0]
      if stable_version == toolchain_ver
        toolchain_arg += stable_toolchain_arg
      endif
    endif
    if toolchain_arg == []
      toolchain_arg += ['--toolchain-version', toolchain_ver]
    endif
    toolchain_arg += toolchain_arg + build_triplet_arg
  endif

  if target_arg == []
    default_host = run_command(
      rustc_query_args + ['--query=default-host-toolchain'] + toolchain_arg,
      capture: true,
      check: true
    )
    if default_host.stderr() != ''
      error('error occurred when querying default toolchain: @0@'.format(default_host.stderr().split()[0]))
    endif
    default_rustc_toolchain = default_host.stdout().split()[0]

    # If the default Rust target triplet does not match the triplet that we want to build for
    # pass in a --target argument to RustC/Cargo that best matches what we are building for,
    # if we didn't use -Dtriplet=... in the Meson setup line.  Do the same if a toolchain version
    # is requested
    if default_rustc_toolchain != rustc_target
      target_arg += ['--target', rustc_target]
    endif
  endif
endif

default_overrides = []
if host_system in ['windows', 'linux']
  rustc_query_native_static_libs_args = rustc_query_args
  if target_arg != []
    rustc_query_native_static_libs_args += target_arg
  endif
  if toolchain_arg != []
    rustc_query_native_static_libs_args += toolchain_arg
  endif
  rustc_query_native_static_libs_args += ['--query=native-static-libs']

  native_libs = run_command(
    rustc_query_native_static_libs_args,
    capture: true,
    check: true
  )

  if native_libs.stderr() != ''
    error(native_libs.stderr())
  endif

  foreach i: native_libs.stdout().split()
    if 'msvcrt' in i
      if i.endswith('.lib') # pre Rust 1.83
        default_overrides = [
          'b_vscrt=md@0@'.format(i.substring(0, -4).split('msvcrt')[1])
        ]
      else # /DEFAULTLIB:<crt goes here, minus the .lib extension>
        default_overrides = [
          'b_vscrt=md@0@'.format(i.split('msvcrt')[1])
        ]
      endif
    else
      private_libraries += cc.find_library(i, required: get_option('default_library') != 'shared')
    endif
  endforeach
endif

private_libraries += [m_dep]
private_dependencies += network_deps
# appleframeworks cannot be exposed to pkg-config
library_dependencies += foundation_dep
library_dependencies += other_library_dependencies
library_dependencies += private_libraries
library_dependencies += private_dependencies

cargo_toml = meson.project_source_root() / 'Cargo.toml'

if get_option('default_library') == 'static'
  # Tell the pkg-config crate to think of all libraries as static
  extra_env.set('PKG_CONFIG_ALL_STATIC', '1')
  # Tell the system-deps crate to process linker flag for static deps
  extra_env.set('SYSTEM_DEPS_LINK', 'static')
  # Allow cross-compilation
  extra_env.set('PKG_CONFIG_ALLOW_CROSS', 'static')
endif

pkg_config = find_program('pkgconfig', 'pkg-config', 'pkgconf', required: true)
extra_env.set('PKG_CONFIG', pkg_config.full_path())

pkg_config_path = get_option('pkg_config_path')
if pkg_config_path.length() > 0
  extra_env.set('PKG_CONFIG_PATH', pkg_config_path)
endif

# Make sure dependencies provided by the superproject are propagated
# (or ours, if there are wraps within this tree)
if meson.is_subproject()
  extra_env.prepend('PKG_CONFIG_PATH', meson.global_build_root() / 'meson-uninstalled')
endif

# Set up the environment that will be fed to the build
if host_system in ['darwin', 'ios']
  var = 'DYLD_LIBRARY_PATH'
elif host_system in ['windows', 'cygwin']
  var = 'PATH'
else
  var = 'LD_LIBRARY_PATH'
endif

current_library_path = run_command(
  [
    python,
    '-c',
    'import os; print(os.environ["@0@"]) if "@0@" in os.environ else ""'.format(var)
  ],
  capture: true,
  check: true,
)

extra_env.set(var, current_library_path.stdout().strip())
extra_env.prepend(var, meson.project_build_root() / 'rsvg')
if host_system == 'windows'
  foreach i: library_dependencies_sole + private_dependencies
    x = i.get_variable(pkgconfig: 'bindir', default_value: '')
    if x != ''
      extra_env.prepend('PATH', x)
    endif
  endforeach
endif

# used in librsvg/meson.build.  This is so that '#include <librsvg/rsvg.h>` will pick
# up exactly that file from the source tree.
includeinc = include_directories('include')

# Set the suffixes up
if host_system == 'windows'
  lib_prefix = is_msvc_style ? '' : 'lib'
  ext_dynamic = 'dll'
  ext_static = is_msvc_style ? 'lib' : 'a'
  ext_exe = '.exe'
elif host_system in ['darwin', 'ios']
  lib_prefix = 'lib'
  ext_dynamic = 'dylib'
  ext_static = 'a'
  ext_exe = ''
else
  lib_prefix = 'lib'
  ext_dynamic = 'so'
  ext_static = 'a'
  ext_exe = ''
endif

cargo_dylib_prefix = lib_prefix
if cc.has_define('__MINGW32__')
  cargo_dylib_prefix = ''
endif

cargo_wrapper_args = [
  '--cargo', cargo.full_path(),
  '--manifest-path', cargo_toml,
  '--project-build-root', meson.project_build_root(),
  '--prefix', get_option('prefix'),
  '--libdir', get_option('libdir'),
]

if not get_option('debug')
  cargo_wrapper_args += ['--release']
endif
optimization = get_option('optimization')
if optimization in ['0', '1', '2', '3', 's']
  cargo_wrapper_args += ['--optimization', optimization]
elif optimization != 'plain' # g
  cargo_wrapper_args += ['--optimization', '1']
  # plain: https://github.com/mesonbuild/meson/issues/7194
endif

if get_option('b_lto')
  if get_option('b_lto_mode') == 'default'
    cargo_wrapper_args += ['--lto', 'fat']
  else
    cargo_wrapper_args += ['--lto', 'thin']
  endif
endif

if target_arg != []
  cargo_wrapper_args += target_arg
endif
if toolchain_arg != []
  cargo_wrapper_args += toolchain_arg
endif

# avif support with dav1d

if dav1d_dep.found()
  avif_feature_args = ['--avif']
else
  avif_feature_args = []
endif

# gdk-pixbuf support

if pixbuf_dep.found()
  pixbuf_feature_args = ['--pixbuf']
else
  pixbuf_feature_args = []
endif

subdir('include')
subdir('librsvg-c')
subdir('rsvg')
if build_tests
  subdir('librsvg-c/tests-c')
endif
if build_rsvg_convert.allowed()
  subdir('rsvg_convert')
endif
if build_pixbuf_loader.allowed()
  subdir('gdk-pixbuf-loader')
endif
if build_docs.allowed()
  subdir('doc')
endif

# The Vala generator must reside in the main folder since Meson isn't yet
# clever enough to take a list of files.
# See https://github.com/mesonbuild/meson/blob/b290a8254122f53d6477b7ba24366d81cfc5e99c/mesonbuild/modules/gnome.py#L2150-L2152
if build_gir.allowed() and build_vala.allowed()
  vala = gnome.generate_vapi(
    'librsvg-@0@'.format(librsvg_api_version),
    packages: vala_includes,
    metadata_dirs: [meson.current_source_dir()],
    sources: ['Rsvg-@0@-custom.vala'.format(librsvg_api_version), rsvg_gir[0]],
    install: true,
  )
endif

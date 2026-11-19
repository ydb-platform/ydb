includes = files(
    'rsvg.h',
    'rsvg-cairo.h',
    'rsvg-pixbuf.h'
)

features_data = configuration_data()
features_data.set10('LIBRSVG_HAVE_PIXBUF', pixbuf_dep.found())

features_header = configure_file(
    input: 'rsvg-features.h.in',
    output: 'rsvg-features.h',
    configuration: features_data
)

header_version = meson.project_version().split('.')

version_header = configure_file(
    input: 'rsvg-version.h.in',
    output: 'rsvg-version.h',
    configuration: {
        'LIBRSVG_MAJOR_VERSION': header_version[0],
        'LIBRSVG_MINOR_VERSION': header_version[1],
        'LIBRSVG_MICRO_VERSION': header_version[2],
        'PACKAGE_VERSION': meson.project_version(),
    }
)

includes += [features_header, version_header]
install_headers(
    includes,
    subdir: '@0@/librsvg'.format(librsvg_pc),
)

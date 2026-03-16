from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    with self.yamakes["."] as m:
        m.CFLAGS.remove("-DHAVE_UNISTD_H")


libheif = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libheif",
    nixattr="libheif",
    copy_sources=[
        "libheif/plugins_windows.h",
    ],
    install_targets=["heif"],
    disable_includes=[
        "codecs/uncompressed/unc_dec.h",
        "codecs/uncompressed/unc_enc.h",
        "heif_emscripten.h",
        "mini.h",
        "plugins/decoder_dav1d.h",
        "plugins/decoder_ffmpeg.h",
        "plugins/decoder_jpeg.h",
        "plugins/decoder_openh264.h",
        "plugins/decoder_openjpeg.h",
        "plugins/decoder_uncompressed.h",
        "plugins/decoder_vvdec.h",
        "plugins/decoder_webcodecs.h",
        "plugins/encoder_jpeg.h",
        "plugins/encoder_kvazaar.h",
        "plugins/encoder_openjpeg.h",
        "plugins/encoder_openjph.h",
        "plugins/encoder_rav1e.h",
        "plugins/encoder_svt.h",
        "plugins/encoder_uncompressed.h",
        "plugins/encoder_uvg266.h",
        "plugins/encoder_vvenc.h",
        "plugins/encoder_x264.h",
        "sharpyuv/sharpyuv.h",
        "sharpyuv/sharpyuv_csp.h",
        # additional compression codecs
        "brotli/",
        "zlib.h",
    ],
    platform_dispatchers=["config.h"],
    post_install=post_install,
)

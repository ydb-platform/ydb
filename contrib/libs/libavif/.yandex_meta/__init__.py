from devtools.yamaker.project import CMakeNinjaNixProject


def post_install(self):
    """Пост-обработка после установки"""
    with self.yamakes["."] as m:
        # Добавить зависимость на libsharpyuv для поддержки ImageRGBToYUVLibSharpYUV
        m.PEERDIR.add("contrib/libs/libwebp/sharpyuv")
        # Добавить путь к заголовкам sharpyuv
        m.ADDINCL.add("contrib/libs/libwebp")


libavif = CMakeNinjaNixProject(
    arcdir="contrib/libs/libavif",
    nixattr="libavif",
    install_targets=["avif"],
    post_install=post_install,
)

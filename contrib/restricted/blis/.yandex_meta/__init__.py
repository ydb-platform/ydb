import shutil

from devtools.yamaker.project import NixProject


def blis_post_build(self):
    with self.yamakes["."] as m:
        m.ADDINCL.remove("${ARCADIA_BUILD_ROOT}/contrib/restricted/blis/include/x86_64")
        m.ADDINCL.extend(
            [
                "contrib/restricted/blis",
                "contrib/restricted/blis/frame/base",
                "contrib/restricted/blis/frame/thread",
            ]
        )
        for define in (
            "-DBLIS_CNAME=bulldozer",
            "-DBLIS_CNAME=excavator",
            "-DBLIS_CNAME=haswell",
            "-DBLIS_CNAME=knl",
            "-DBLIS_CNAME=penryn",
            "-DBLIS_CNAME=piledriver",
            "-DBLIS_CNAME=sandybridge",
            "-DBLIS_CNAME=skx",
            "-DBLIS_CNAME=steamroller",
            "-DBLIS_CNAME=zen",
            "-DBLIS_CNAME=zen2",
        ):
            m.CFLAGS.remove(define)
        for prefix in (
            "config/bulldozer",
            "config/excavator",
            "config/haswell",
            "config/knl",
            "config/penryn",
            "config/piledriver",
            "config/sandybridge",
            "config/skx",
            "config/steamroller",
            "config/zen",
            "config/zen2",
            "kernels",
        ):
            shutil.rmtree(self.dstdir + f"/{prefix}")
            m.SRCS = [src for src in m.SRCS if not src.startswith(prefix)]
        # Нужно руками сгенерить blis.h, иначе сходит с ума configure, при виде кучи неизвестных заголовков
        m.RUN_PROGRAM = []


blis = NixProject(
    arcdir="contrib/restricted/blis",
    nixattr="blis",
    license="BSD-3-Clause",
    disable_includes=[
        "hbwmalloc.h",
        "bli_sandbox.h",
        "bli_family_intel64.h",
        "bli_family_amd64.h",
        "bli_family_knc.h",
        "bli_family_thunderx2.h",
        "bli_family_cortexa57.h",
        "bli_family_cortexa53.h",
        "bli_family_cortexa15.h",
        "bli_family_cortexa9.h",
        "bli_family_power10.h",
        "bli_family_power9.h",
        "bli_family_power7.h",
        "bli_family_bgq.h",
        "bli_kernels_knc.h",
        "bli_kernels_armsve.h",
        "bli_kernels_armv8a.h",
        "bli_kernels_armv7a.h",
        "bli_kernels_power10.h",
        "bli_kernels_power9.h",
        "bli_kernels_power7.h",
        "bli_kernels_bgq.h",
        "bli_family_skx.h",
        "bli_family_knl.h",
        "bli_family_haswell.h",
        "bli_family_sandybridge.h",
        "bli_family_penryn.h",
        "bli_family_zen2.h",
        "bli_family_zen.h",
        "bli_family_excavator.h",
        "bli_family_steamroller.h",
        "bli_family_piledriver.h",
        "bli_family_bulldozer.h",
        "bli_kernels_skx.h",
        "bli_kernels_knl.h",
        "bli_kernels_haswell.h",
        "bli_kernels_sandybridge.h",
        "bli_kernels_penryn.h",
        "bli_kernels_zen2.h",
        "bli_kernels_zen.h",
        "bli_kernels_piledriver.h",
        "bli_kernels_bulldozer.h",
    ],
    post_build=blis_post_build,
)

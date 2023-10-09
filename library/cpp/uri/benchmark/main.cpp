#include <library/cpp/uri/uri.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/vector.h>

const TString URLS[] = {
    "http://www.TEST.Ru:80/InDex.html",
    "www.ya.ru/index.html",
    "https://workplace.z.yandex-team.ru/di#vertical=drive&datePreset=week",
    "https://warden.z.yandex-team.ru/components/web/report?filter_type=action_items&filter_status=total&filter_period=review",
    "https://meduza.io/news/2021/05/01/italiya-vozobnovila-vydachu-turisticheskih-viz-v-moskve",
    "https://gcc.gnu.org/projects/cxx-status.html#cxx20",
    "https://github.com/llvm/llvm-project/commits/main/libcxx",
    "https://photos.google.com/share/AF1QipNi8VN2pw2Ya_xCV8eFgzEZmiXDy1-GwhXbqFtvXoH3HypF10as9puV8FdoVZpOZA?key=WkZjQTIxQTM5a01oZkNUYTE2ZllKTVJKZk1CMTR3",
    "https://mag.auto.ru/article/ladasolaris/?from=mag_web_block&utm_campaign=ladasolaris&utm_content=populyarnoe&utm_source=mag_web_block&utm_medium=cpc",
    "https://yabs.yandex.ru/count/WZ4ejI_zODW1FH40j1nmE7kaiN1MJWK0s08GWY0nuM6EO000000useqKG0H846344d30nU21pYI00GcG0VpCzQaucBW1mCNWWOR1co7e0QG2y0A-meRx0v03yCE0RzqD-0Jozzu1Y0Nozzu1a0Nozzu1m0Nvks_D2-U0-KK5Iga7StZ0vgZ8Ao2m1u20c0ou1-u9q0SIW870W822W07u2DoYy82d4W10oQeB4EBQlCbpU0002zaBbLJ1w0lozzu1y0i6w0oNm808WWuaCaOrC30oGaKjHaCoHIqqDpWoBJX1HZajCaD3HJGtDKH3Gp7Dbvo7cB_HWagW3i24FO0Gm-3DmA8G0w7W4e606EaIQREKnW19jq8oc1C8g1E2WkxBsSo3dXRW4_BttW6W5FBttW6e5FBttW7G5EUCwadO583Nge06w1IC0j0LWDUgW0RO5S6AzkoZZxpyO_2G5i41e1RGWVs31iaMWHUe5md05xGIs1V0X3sm68AikOG6q1WG-1ZKfU3zXERBdee1W1cG6G6W6Qe3i1cu6T8P4dbXOdDVSsLoTcLoBt8rCZGjCkWPWC83y1c0mWE16l__eqZnoGHLc1hyy8W2i1hotyIEmftqxKJr6W40000R02tR-6NBeHRAQn6iQSWtbmq2DFDwGK8a9Muu2wvTOhNWE4vj3Fy-LbbUXS4Y0NLWZWE9MX1ZSNyOanc8rS-k0u1LdAMo0O-fHvO33T0LBwYZBzcF7h0qb3q0~1",
    "https://yandex.ru/pogoda/nowcast/?utm_campaign=alert&utm_content=alert_separate&utm_medium=web&utm_source=home&utm_term=nowcast&morda_geolocation=1",
    "https://an.yandex.ru/count/WluejI_zOE02fHS052S_YplH_yoFzGK0u0CGWY0nncwAO000000uYgLImfs4aOKAW06Suha2Y07-gFqAa07-vu2bpu20W0AO0VxdWALFe07Ug07Uk076nFll8S010jW1w9_VcG7W0Soxo1te0Ue40Q02vAG1y0Aasfk_0SOA-0IzX9W1Y0MzX9W1a0Nav9y1e0MCiIwe1ShJ9h05ojCck0NkpoZ9qoOhae0mkgnJ5wa7cB8OEOwtEX-m1u20a2Iu1u05ibB92YPyFmDpJFK_D7ddf9Yo002blSLG6C7e2xs4c07m2mc83BIDthu1gGpzGrRLHHZfF-WCbmBW3OA0mC60288E93OtGqOmC4L4BJ4sDaCjD4P5H2r1EJX5BKKuCZCvGZX4DqGtgwI2XAENwwcOvUBgu_6jdH_P3u0Gz8sO7OWGpz-nX0e1eU0HoV740-WHhS3enS2ScyBwO3_lGEaI3kz-5M87zWKoc1C4g1Eli-_mek7ml1RW4-xFA8WKqRAuZvJoWDi8e1JkpoYe5EJadudj_uC6w1I40iWLhPEErFa4q1MEz9w41jWLmOhsxAEFlFnZyA0Mq87zWmR95j0Mj8tUlW615vWNWfsO8wWN2RWN0S0NjHBO5y24FUWN0PaOe1WBi1ZIlwc41hWO0j0O4FWOeQI_pOMKhhOPW1c96MWs1G000000a1a1e1cg0x0Pk1d_0T8P4dbXOdDVSsLoTcLoBt8rCZCjCkWPWC83y1c0mWE16l__fs_4tYo3a1g0W06O6l70j06m6hkouUoAwFByhm6u6W7r6W4000226nmnDZ4vDZWrC3OnBZasE30pBZWrDJSmBZOuDJSt9I1HX8PYb34CCgLmxEvDy50h8PoKR4c2QKHEe8YaRoCEXF5m_E1rNC99Bpm6wIRbNGAU6GiQ3qguwKm8cq7la7pmUZxo079vS26KW_bTaCPY9QnjKrUHau50oq6yQCyn5TW0kBtjBWG2fE3FRk2DVwuirPJ_lvLb3GlkDKAujdLv~1?stat-id=1",
    "https://rasp.yandex.ru/?utm_source=yamain&utm_medium=geoblock&utm_campaign=main",
    "https://dsp-rambler.ru/click?url=hs31lTJpNQdz5IfhdiQZitCij144sWuuLcC3jopSDRPYYlPZNuD1x6OEJg74u0nbP0J0oXWXw7awYsCTgG7Cr*koGqxBBhOnzB5aZ6aLfCVVGlIuP3L194y025VLJAsK04sZwSvJbxEYfnbOWKPgQHOf7c8Gqkope95-kr--aTxpsg18jiwVdEPBENe3F2Iahm3yL*3vvzt-t7Vq6bANhL3DiJglfLw2WVd4tAhoAepFV*QybPJFoGHbdOWGMTyzpWMxWPLKb7MRFQWj1K*58qPyfNkfyQ72vvzjviirTM57xng8Cxbi08-HzM5x7imDOYIY8EvXOqfU3Q0KWsyO1RC1yHHVinVxthuIb16zLjg2aoYFpz-OLiTYhlmk1vyK9t9fLz8*Oiez9i*7TqIwcWZyX5gUFuOOJ4sTWbeQLe6IQEShvKIj7v9yBZRLkRmdHLfrREuTNpBMBtRG80MIxwXyt6SjjFOhSKtK-yDA19Wawzgw9fNOy9DW0TDAehQkPUTz*5-htmszyUqJWk5ovqoyHV3acnOI2-klqMCMfU9w3*GOYS0CuNTrggGCQH376EaeQthtwiUcabSarBEocGEsW7n27kIsrtYh2-SZwPvKC1Ek3dg35nuEO-MWNPMmqJvAhBGXHF*EQkcB3eLUEluJmwTxqPRv00M4PNgdYsYsgKYPU6MMJTxbH7fA*Q2vA7WErGONTSzhSOeNLPP4vR9WalRvzllDB3XH4bNx6Pleb3ZfWmoYTNN0Lux3-VxOSjIvDDGzMirfOVPKZB4qVQWsP4WHCrRgsijW43cKGhcQ0dPORYO5v0xhzMjoZ0qDEsaiXBkfRXccnQ3QGaXk2PC1vu*Zlj0qgJfO5i8e66z4HEiMWRF4JH8ZsbZqrUzKXX-WpPQuVA0MOslhzq8m3KS3UIEurWCn80utY4AWCuHzGooJbb*PcHhYMqIBbLcJW76RK9HQjI8Bxiu4C9wECXeWIqeVuHzbmb7PGizIDVwg-g8I-zrBgd8OH3kWtC09YwSB-F9wOHrrcBG*Sv6fdnwubW1ndv0V1jsok2DhMT9IFBOoa-brtWYIdttkRs2J4m*Ai5IgVOagS2wyboiHKptd7aQ7j1YnJENZbFfs2nSwvPTvvSA8w-vCJHo-xEW6tPWaAOrVVRZscjNb4HovTUKBhxrCm8cZYw0ZahlRWGDpB0QkiI-xSv7YdYzoAQMvEOF8h*MKTn1Had8cI2FJ3WtcaT3siShD*APePK6dwqGsNJRz3lfbeX*hykpwK8kTumfS6z51bv06bjahc*fo1vjNt8ivt2BJPWqnGkYH9-8r76iBia7d1zKKnzfqk-mu3m9eP0kiKkoqMKaugV2muZlt5h4ps29ikmTIc-8vYtwHOdLDay7PUhTC4tKepBVRZh6nXrIa5POmkVNV7hVfeoMXqlid2B-2LM3CWCo*W2r23aefVN8mi3t-dnWNUlVgurAc*674C76Py8Qdr0*EJqmYjhrQw6jbm*nB3O-kd1aYqWXWC3Msg1a5r9sRu1WVLKzmwwzjPX6b44R5ULVhu1OqH6*O7hFIbN584lUhWM6g1nWFqhwhN3**Bam802sRvZjguTILo*UAH7WW1DRRG5MsTs-ZP3tOFMkQKWuJ3LRLDXtnkyN25S0LYEmH8R0vTstUmFwafeJSmm90Iuseu9DKArqrb1Wn2cZv2zgAEYy66U7kkBTQSC76WlwBJTVpoK6MUohrEll23wivbnhNaGzaSe6kWRq1ItpFkqv9gXkCAAAAuty8CgAAAAA",
    "https://news.rambler.ru/moscow_city/46342947-pogoda-v-moskve-sinoptiki-poobeschali-moskvicham-silnyy-liven-blizhayshey-nochyu/?utm_source=head&utm_campaign=self_promo&utm_medium=news&utm_content=news",
};

Y_CPU_BENCHMARK(Parsing, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (auto&& url : URLS) {
            NUri::TUri uri;
            auto parseResult = uri.Parse(url, uri.FeaturesAll);
            Y_DO_NOT_OPTIMIZE_AWAY(parseResult);
            Y_ABORT_UNLESS(parseResult == NUri::TState::ParsedOK, "cannot parse %s: %d", url.c_str(), static_cast<ui32>(parseResult));
        }
    }
}

Y_CPU_BENCHMARK(ParsingAndCopying, iface) {
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        for (auto&& url : URLS) {
            NUri::TUri uri;
            auto parseResult = uri.Parse(url, uri.FeaturesAll);
            Y_ABORT_UNLESS(parseResult == NUri::TState::ParsedOK, "cannot parse %s: %d", url.c_str(), static_cast<ui32>(parseResult));
            auto copy = uri;
            Y_DO_NOT_OPTIMIZE_AWAY(copy);
        }
    }
}

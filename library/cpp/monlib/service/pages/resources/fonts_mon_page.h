#pragma once

#include <library/cpp/monlib/service/pages/resource_mon_page.h>

namespace NMonitoring {
    struct TBootstrapFontsEotMonPage: public TResourceMonPage {
        TBootstrapFontsEotMonPage()
            : TResourceMonPage("static/fonts/glyphicons-halflings-regular.eot", "static/fonts/glyphicons-halflings-regular.eot", FONT_EOT, true)
        {
        }
    };

    struct TBootstrapFontsSvgMonPage: public TResourceMonPage {
        TBootstrapFontsSvgMonPage()
            : TResourceMonPage("static/fonts/glyphicons-halflings-regular.svg", "static/fonts/glyphicons-halflings-regular.svg", SVG, true)
        {
        }
    };

    struct TBootstrapFontsTtfMonPage: public TResourceMonPage {
        TBootstrapFontsTtfMonPage()
            : TResourceMonPage("static/fonts/glyphicons-halflings-regular.ttf", "static/fonts/glyphicons-halflings-regular.ttf", FONT_TTF, true)
        {
        }
    };

    struct TBootstrapFontsWoffMonPage: public TResourceMonPage {
        TBootstrapFontsWoffMonPage()
            : TResourceMonPage("static/fonts/glyphicons-halflings-regular.woff", "static/fonts/glyphicons-halflings-regular.woff", FONT_WOFF, true)
        {
        }
    };

}

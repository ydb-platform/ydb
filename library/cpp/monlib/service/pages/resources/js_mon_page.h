#pragma once

#include <library/cpp/monlib/service/pages/resource_mon_page.h>

namespace NMonitoring {
    struct TBootstrapJsMonPage: public TResourceMonPage {
        TBootstrapJsMonPage()
            : TResourceMonPage("static/js/bootstrap.min.js", "static/js/bootstrap.min.js", JAVASCRIPT, true)
        {
        }
    };

    struct TJQueryJsMonPage: public TResourceMonPage {
        TJQueryJsMonPage()
            : TResourceMonPage("static/js/jquery.min.js", "static/js/jquery.min.js", JAVASCRIPT, true)
        {
        }
    };

}

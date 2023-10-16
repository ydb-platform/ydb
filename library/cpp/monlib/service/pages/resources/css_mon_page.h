#pragma once

#include <library/cpp/monlib/service/pages/resource_mon_page.h>

namespace NMonitoring {
    struct TBootstrapCssMonPage: public TResourceMonPage {
        TBootstrapCssMonPage()
            : TResourceMonPage("static/css/bootstrap.min.css", "static/css/bootstrap.min.css", CSS, true)
        {
        }
    };

}

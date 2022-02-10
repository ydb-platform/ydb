#pragma once

#include "pre_mon_page.h"

namespace NMonitoring {
    // internal diagnostics page
    struct TDiagMonPage: public TPreMonPage {
        TDiagMonPage()
            : TPreMonPage("diag", "Diagnostics Page")
        {
        }

        void OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest&) override;
    };

}

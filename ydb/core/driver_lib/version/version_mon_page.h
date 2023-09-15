#pragma once

#include <library/cpp/monlib/service/pages/version_mon_page.h>
#include "version.h"

namespace NMonitoring {
    struct TYdbVersionMonPage: public TVersionMonPage {
        TYdbVersionMonPage(const TString& path = "ver", const TString& title = "Version")
            : TVersionMonPage(path, title)
        {
        }

        void OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest& req) override {
            TVersionMonPage::OutputText(out, req);
            TString compatibilityInfo(NKikimr::CompatibilityInfo.PrintHumanReadable());
            out << "\n" << compatibilityInfo;
            if (!compatibilityInfo.EndsWith("\n")) {
                out << "\n";
            }
        }
    };

}

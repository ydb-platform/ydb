#pragma once

#include "html_mon_page.h"

namespace NMonitoring {
    struct TPreMonPage: public THtmlMonPage {
        TPreMonPage(const TString& path,
                    const TString& title = TString(),
                    bool preTag = true,
                    bool outputTableSorterJsCss = false)
            : THtmlMonPage(path, title, outputTableSorterJsCss)
            , PreTag(preTag)
        {
        }

        void OutputContent(NMonitoring::IMonHttpRequest& request) override;

        // hook to customize output
        virtual void BeforePre(NMonitoring::IMonHttpRequest& request);

        // put your text here
        virtual void OutputText(IOutputStream& out, NMonitoring::IMonHttpRequest&) = 0;

        const bool PreTag;
    };

}

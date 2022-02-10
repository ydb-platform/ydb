#pragma once

#include "mon_page.h"

namespace NMonitoring {
    struct THtmlMonPage: public IMonPage {
        THtmlMonPage(const TString& path,
                     const TString& title = TString(),
                     bool outputTableSorterJsCss = false)
            : IMonPage(path, title)
            , OutputTableSorterJsCss(outputTableSorterJsCss)
        {
        }

        void Output(NMonitoring::IMonHttpRequest& request) override;

        void NotFound(NMonitoring::IMonHttpRequest& request) const;
        void NoContent(NMonitoring::IMonHttpRequest& request) const;

        virtual void OutputContent(NMonitoring::IMonHttpRequest& request) = 0;

        bool OutputTableSorterJsCss;
    };

}

#pragma once

#include "mon_page.h"

#include <library/cpp/resource/resource.h>

namespace NMonitoring {
    struct TResourceMonPage: public IMonPage {
    public:
        enum EResourceType {
            BINARY,
            TEXT,
            JSON,
            CSS,
            JAVASCRIPT,

            FONT_EOT,
            FONT_TTF,
            FONT_WOFF,
            FONT_WOFF2,

            PNG,
            SVG
        };

        TResourceMonPage(const TString& path, const TString& resourceName,
                         const EResourceType& resourceType = BINARY, const bool isCached = false)
            : IMonPage(path, "")
            , ResourceName(resourceName)
            , ResourceType(resourceType)
            , IsCached(isCached)
        {
        }

        void Output(NMonitoring::IMonHttpRequest& request) override;

        void NotFound(NMonitoring::IMonHttpRequest& request) const;

    private:
        TString ResourceName;
        EResourceType ResourceType;
        bool IsCached;
    };

}

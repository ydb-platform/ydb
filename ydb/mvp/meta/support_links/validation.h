#pragma once

#include "common.h"
#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

class TConfigValidation {
public:
    void Validate(const TSupportLinkEntryConfig& linkConfig, const TGrafanaSupportConfig&, TStringBuf where) const {
        if (linkConfig.Source.empty()) {
            ythrow yexception() << where << ": source is required";
        }

        ythrow yexception() << where
                            << ": support_links sources are not available in this build"
                            << " (source=" << linkConfig.Source << ")";
    }

    static const TConfigValidation& Default() {
        static const TConfigValidation validation;
        return validation;
    }
};

} // namespace NMVP::NSupportLinks

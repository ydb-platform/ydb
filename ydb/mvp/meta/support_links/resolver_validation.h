#pragma once

#include <ydb/mvp/meta/mvp.h>

#include <util/generic/yexception.h>

namespace NMVP::NSupportLinks {

class TLinkSourceConfigValidators {
public:
    void Validate(const TSupportLinkEntryConfig& linkConfig, TStringBuf where) const {
        if (linkConfig.GetSource().empty()) {
            ythrow yexception() << where << ": source is required";
        }

        ythrow yexception() << where
                            << ": support_links sources are not available in this build"
                            << " (source=" << linkConfig.GetSource() << ")";
    }

    static const TLinkSourceConfigValidators& Default() {
        static const TLinkSourceConfigValidators validators;
        return validators;
    }
};

} // namespace NMVP::NSupportLinks

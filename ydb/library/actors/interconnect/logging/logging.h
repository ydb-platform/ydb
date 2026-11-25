#pragma once

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

namespace NActors {
    class TInterconnectLoggingBase {
    protected:
        const TString LogPrefix;

    public:
        TInterconnectLoggingBase() = default;

        TInterconnectLoggingBase(const TString& prefix)
            : LogPrefix(prefix)
        {
        }

        void SetPrefix(TString logPrefix) const {
            logPrefix.swap(const_cast<TString&>(LogPrefix));
        }
    };
}

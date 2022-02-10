#pragma once

#include "preprocessor.h"
#include "signature.h"
#include "param_traits.h"

#include <library/cpp/lwtrace/protos/lwtrace.pb.h>

namespace NLWTrace {
    // Common class for all events
    struct TEvent {
        const char* Name;
        const char* Groups[LWTRACE_MAX_GROUPS + 1];
        TSignature Signature;

        const char* GetProvider() const {
            return Groups[0];
        }

        void ToProtobuf(TEventPb& pb) const {
            pb.SetName(Name);
            for (const char* const* gi = Groups; *gi != nullptr; gi++) {
                pb.AddGroups(*gi);
            }
            Signature.ToProtobuf(pb);
        }
    };

}

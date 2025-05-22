#pragma once

#include <yql/essentials/sql/v1/complete/core/input.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    struct TUseContext {
        TString Provider;
        TString Cluster;
    };

    struct TGlobalContext {
        TMaybe<TUseContext> Use;
    };

    class IGlobalAnalysis {
    public:
        using TPtr = THolder<IGlobalAnalysis>;

        virtual ~IGlobalAnalysis() = default;
        virtual TGlobalContext Analyze(TCompletionInput input) = 0;
    };

    IGlobalAnalysis::TPtr MakeGlobalAnalysis();

} // namespace NSQLComplete

#pragma once

#include <yql/essentials/sql/v1/complete/core/environment.h>
#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/sql/v1/complete/core/name.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TUseContext {
        TString Provider;
        TString Cluster;
    };

    struct TColumnContext {
        TVector<TTableId> Tables;

        friend bool operator==(const TColumnContext& lhs, const TColumnContext& rhs) = default;
    };

    struct TGlobalContext {
        TMaybe<TUseContext> Use;
        TVector<TString> Names;
        TMaybe<TString> EnclosingFunction;
        TMaybe<TColumnContext> Column;
    };

    class IGlobalAnalysis {
    public:
        using TPtr = THolder<IGlobalAnalysis>;

        virtual ~IGlobalAnalysis() = default;
        virtual TGlobalContext Analyze(TCompletionInput input, TEnvironment env) = 0;
    };

    IGlobalAnalysis::TPtr MakeGlobalAnalysis();

} // namespace NSQLComplete

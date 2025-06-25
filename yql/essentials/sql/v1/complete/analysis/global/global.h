#pragma once

#include <yql/essentials/sql/v1/complete/core/environment.h>
#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/sql/v1/complete/core/name.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NSQLComplete {

    struct TUseContext {
        TString Provider;
        TString Cluster;
    };

    struct TColumnId {
        TString TableAlias;
        TString Name;

        friend bool operator<(const TColumnId& lhs, const TColumnId& rhs);
        friend bool operator==(const TColumnId& lhs, const TColumnId& rhs) = default;
    };

    struct TColumnContext {
        TVector<TAliased<TTableId>> Tables;
        TVector<TColumnId> Columns;

        TVector<TAliased<TTableId>> TablesWithAlias(TStringBuf alias) const;
        bool IsAsterisk() const;
        TColumnContext Renamed(TStringBuf alias) &&;

        friend bool operator==(const TColumnContext& lhs, const TColumnContext& rhs) = default;
        friend TColumnContext operator|(TColumnContext lhs, TColumnContext rhs);

        static TColumnContext Asterisk();
    };

    struct TGlobalContext {
        TMaybe<TUseContext> Use;
        TVector<TString> Names;
        TMaybe<TString> EnclosingFunction;
        TMaybe<TColumnContext> Column;
    };

    // TODO(YQL-19747): Make it thread-safe to make ISqlCompletionEngine thread-safe.
    class IGlobalAnalysis {
    public:
        using TPtr = THolder<IGlobalAnalysis>;

        virtual ~IGlobalAnalysis() = default;
        virtual TGlobalContext Analyze(TCompletionInput input, TEnvironment env) = 0;
    };

    IGlobalAnalysis::TPtr MakeGlobalAnalysis();

} // namespace NSQLComplete

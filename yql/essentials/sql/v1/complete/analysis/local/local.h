#pragma once

#include <yql/essentials/sql/v1/complete/core/name.h>
#include <yql/essentials/sql/v1/complete/sql_complete.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    struct TEditRange {
        size_t Begin = 0;
        size_t Length = 0;
    };

    struct TLocalSyntaxContext {
        using TKeywords = THashMap<TString, TVector<TString>>;

        struct TPragma {
            TString Namespace;
        };

        struct TFunction {
            TString Namespace;
            ENodeKind ReturnType = ENodeKind::Any;
        };

        struct THint {
            EStatementKind StatementKind;
        };

        struct TCluster {
            TString Provider;
        };

        struct TObject {
            TString Provider;
            TString Cluster;
            TString Path;
            THashSet<EObjectKind> Kinds;

            bool HasCluster() const {
                return !Cluster.empty();
            }

            bool IsDeferred() const {
                return Kinds.empty();
            }
        };

        struct TColumn {
            TString Table;
        };

        struct TQuotation {
            bool AtLhs = false;
            bool AtRhs = false;

            explicit operator bool() const {
                return AtLhs || AtRhs;
            }
        };

        TKeywords Keywords;
        TMaybe<TPragma> Pragma;
        bool Type = false;
        TMaybe<TFunction> Function;
        TMaybe<THint> Hint;
        TMaybe<TObject> Object;
        TMaybe<TCluster> Cluster;
        TMaybe<TColumn> Column;
        bool Binding = false;

        TQuotation IsQuoted;
        TEditRange EditRange;
    };

    // TODO(YQL-19747): Make it thread-safe to make ISqlCompletionEngine thread-safe.
    class ILocalSyntaxAnalysis {
    public:
        using TPtr = THolder<ILocalSyntaxAnalysis>;

        virtual ~ILocalSyntaxAnalysis() = default;
        virtual TLocalSyntaxContext Analyze(TCompletionInput input) = 0;
    };

    ILocalSyntaxAnalysis::TPtr MakeLocalSyntaxAnalysis(
        TLexerSupplier lexer,
        const THashSet<TString>& ignoredRules,
        const THashMap<TString, THashSet<TString>>& disabledPreviousByToken,
        const THashMap<TString, THashSet<TString>>& forcedPreviousByToken);

} // namespace NSQLComplete

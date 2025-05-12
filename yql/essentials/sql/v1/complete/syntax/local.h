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
            bool IsEnclosed = false;
        };

        TKeywords Keywords;
        TMaybe<TPragma> Pragma;
        bool Type = false;
        TMaybe<TFunction> Function;
        TMaybe<THint> Hint;
        TMaybe<TObject> Object;
        TMaybe<TCluster> Cluster;
        TEditRange EditRange;
    };

    class ILocalSyntaxAnalysis {
    public:
        using TPtr = THolder<ILocalSyntaxAnalysis>;

        virtual TLocalSyntaxContext Analyze(TCompletionInput input) = 0;
        virtual ~ILocalSyntaxAnalysis() = default;
    };

    ILocalSyntaxAnalysis::TPtr MakeLocalSyntaxAnalysis(TLexerSupplier lexer);

} // namespace NSQLComplete

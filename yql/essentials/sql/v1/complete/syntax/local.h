#pragma once

#include <yql/essentials/sql/v1/complete/sql_complete.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NSQLComplete {

    struct TLocalSyntaxContext {
        struct TPragma {
            TString Namespace;
        };

        struct TFunction {
            TString Namespace;
        };

        TVector<TString> Keywords;
        std::optional<TPragma> Pragma;
        bool IsTypeName;
        std::optional<TFunction> Function;
    };

    class ILocalSyntaxAnalysis {
    public:
        using TPtr = THolder<ILocalSyntaxAnalysis>;

        virtual TLocalSyntaxContext Analyze(TCompletionInput input) = 0;
        virtual ~ILocalSyntaxAnalysis() = default;
    };

    ILocalSyntaxAnalysis::TPtr MakeLocalSyntaxAnalysis(TLexerSupplier lexer);

} // namespace NSQLComplete

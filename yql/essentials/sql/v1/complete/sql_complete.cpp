#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/text/word.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/syntax/local.h>
#include <yql/essentials/sql/v1/complete/syntax/format.h>

#include <util/generic/algorithm.h>
#include <util/charset/utf8.h>

namespace NSQLComplete {

    class TSqlCompletionEngine: public ISqlCompletionEngine {
    public:
        explicit TSqlCompletionEngine(
            TLexerSupplier lexer,
            INameService::TPtr names,
            ISqlCompletionEngine::TConfiguration configuration)
            : Configuration(std::move(configuration))
            , SyntaxAnalysis(MakeLocalSyntaxAnalysis(lexer))
            , Names(std::move(names))
        {
        }

        TCompletion Complete(TCompletionInput input) override {
            return CompleteAsync(std::move(input)).ExtractValueSync();
        }

        virtual NThreading::TFuture<TCompletion> CompleteAsync(TCompletionInput input) override {
            if (
                input.CursorPosition < input.Text.length() &&
                    IsUTF8ContinuationByte(input.Text.at(input.CursorPosition)) ||
                input.Text.length() < input.CursorPosition) {
                ythrow yexception()
                    << "invalid cursor position " << input.CursorPosition
                    << " for input size " << input.Text.size();
            }

            TLocalSyntaxContext context = SyntaxAnalysis->Analyze(input);

            TStringBuf prefix = input.Text.Head(input.CursorPosition);
            TCompletedToken completedToken = GetCompletedToken(prefix);

            return GetCandidates(std::move(context), completedToken)
                .Apply([completedToken](NThreading::TFuture<TVector<TCandidate>> f) {
                    return TCompletion{
                        .CompletedToken = std::move(completedToken),
                        .Candidates = f.ExtractValue(),
                    };
                });
        }

    private:
        TCompletedToken GetCompletedToken(TStringBuf prefix) const {
            return {
                .Content = LastWord(prefix),
                .SourcePosition = LastWordIndex(prefix),
            };
        }

        NThreading::TFuture<TVector<TCandidate>> GetCandidates(TLocalSyntaxContext context, const TCompletedToken& prefix) const {
            TNameRequest request = {
                .Prefix = TString(prefix.Content),
                .Limit = Configuration.Limit,
            };

            for (const auto& [first, _] : context.Keywords) {
                request.Keywords.emplace_back(first);
            }

            if (context.Pragma) {
                TPragmaName::TConstraints constraints;
                constraints.Namespace = context.Pragma->Namespace;
                request.Constraints.Pragma = std::move(constraints);
            }

            if (context.IsTypeName) {
                request.Constraints.Type = TTypeName::TConstraints();
            }

            if (context.Function) {
                TFunctionName::TConstraints constraints;
                constraints.Namespace = context.Function->Namespace;
                request.Constraints.Function = std::move(constraints);
            }

            if (context.Hint) {
                THintName::TConstraints constraints;
                constraints.Statement = context.Hint->StatementKind;
                request.Constraints.Hint = std::move(constraints);
            }

            if (request.IsEmpty()) {
                return NThreading::MakeFuture<TVector<TCandidate>>({});
            }

            return Names->Lookup(std::move(request))
                .Apply([keywords = std::move(context.Keywords)](NThreading::TFuture<TNameResponse> f) {
                    TNameResponse response = f.ExtractValue();
                    return Convert(std::move(response.RankedNames), std::move(keywords));
                });
        }

        static TVector<TCandidate> Convert(TVector<TGenericName> names, TLocalSyntaxContext::TKeywords keywords) {
            TVector<TCandidate> candidates;
            for (auto& name : names) {
                candidates.emplace_back(std::visit([&](auto&& name) -> TCandidate {
                    using T = std::decay_t<decltype(name)>;
                    if constexpr (std::is_base_of_v<TKeyword, T>) {
                        TVector<TString>& seq = keywords[name.Content];
                        seq.insert(std::begin(seq), name.Content);
                        return {ECandidateKind::Keyword, FormatKeywords(seq)};
                    }
                    if constexpr (std::is_base_of_v<TPragmaName, T>) {
                        return {ECandidateKind::PragmaName, std::move(name.Indentifier)};
                    }
                    if constexpr (std::is_base_of_v<TTypeName, T>) {
                        return {ECandidateKind::TypeName, std::move(name.Indentifier)};
                    }
                    if constexpr (std::is_base_of_v<TFunctionName, T>) {
                        name.Indentifier += "(";
                        return {ECandidateKind::FunctionName, std::move(name.Indentifier)};
                    }
                    if constexpr (std::is_base_of_v<THintName, T>) {
                        return {ECandidateKind::HintName, std::move(name.Indentifier)};
                    }
                }, std::move(name)));
            }
            return candidates;
        }

        TConfiguration Configuration;
        ILocalSyntaxAnalysis::TPtr SyntaxAnalysis;
        INameService::TPtr Names;
    };

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(
        TLexerSupplier lexer,
        INameService::TPtr names,
        ISqlCompletionEngine::TConfiguration configuration) {
        return MakeHolder<TSqlCompletionEngine>(
            lexer, std::move(names), std::move(configuration));
    }

} // namespace NSQLComplete

template <>
void Out<NSQLComplete::ECandidateKind>(IOutputStream& out, NSQLComplete::ECandidateKind kind) {
    switch (kind) {
        case NSQLComplete::ECandidateKind::Keyword:
            out << "Keyword";
            break;
        case NSQLComplete::ECandidateKind::PragmaName:
            out << "PragmaName";
            break;
        case NSQLComplete::ECandidateKind::TypeName:
            out << "TypeName";
            break;
        case NSQLComplete::ECandidateKind::FunctionName:
            out << "FunctionName";
            break;
        case NSQLComplete::ECandidateKind::HintName:
            out << "HintName";
            break;
    }
}

template <>
void Out<NSQLComplete::TCandidate>(IOutputStream& out, const NSQLComplete::TCandidate& candidate) {
    out << "{" << candidate.Kind << ", \"" << candidate.Content << "\"}";
}

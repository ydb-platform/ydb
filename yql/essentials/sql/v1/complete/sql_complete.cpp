#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/text/word.h>
#include <yql/essentials/sql/v1/complete/name/static/name_service.h>
#include <yql/essentials/sql/v1/complete/syntax/local.h>

// FIXME(YQL-19747): unwanted dependency on a lexer implementation
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

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

        TCompletion Complete(TCompletionInput input) {
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

            TVector<TCandidate> candidates;
            EnrichWithKeywords(candidates, std::move(context.Keywords), completedToken);
            EnrichWithNames(candidates, context, completedToken);

            return {
                .CompletedToken = std::move(completedToken),
                .Candidates = std::move(candidates),
            };
        }

    private:
        TCompletedToken GetCompletedToken(TStringBuf prefix) {
            return {
                .Content = LastWord(prefix),
                .SourcePosition = LastWordIndex(prefix),
            };
        }

        void EnrichWithKeywords(
            TVector<TCandidate>& candidates,
            TVector<TString> keywords,
            const TCompletedToken& prefix) {
            for (auto keyword : keywords) {
                candidates.push_back({
                    .Kind = ECandidateKind::Keyword,
                    .Content = std::move(keyword),
                });
            }
            FilterByContent(candidates, prefix.Content);
            candidates.crop(Configuration.Limit);
        }

        void EnrichWithNames(
            TVector<TCandidate>& candidates,
            const TLocalSyntaxContext& context,
            const TCompletedToken& prefix) {
            if (candidates.size() == Configuration.Limit) {
                return;
            }

            TNameRequest request = {
                .Prefix = TString(prefix.Content),
                .Limit = Configuration.Limit - candidates.size(),
            };

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

            if (request.IsEmpty()) {
                return;
            }

            // User should prepare a robust INameService
            TNameResponse response = Names->Lookup(std::move(request)).ExtractValueSync();

            EnrichWithNames(candidates, std::move(response.RankedNames));
        }

        void EnrichWithNames(TVector<TCandidate>& candidates, TVector<TGenericName> names) {
            for (auto& name : names) {
                candidates.emplace_back(std::visit([](auto&& name) -> TCandidate {
                    using T = std::decay_t<decltype(name)>;
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
                }, std::move(name)));
            }
        }

        void FilterByContent(TVector<TCandidate>& candidates, TStringBuf prefix) {
            const auto lowerPrefix = ToLowerUTF8(prefix);
            auto removed = std::ranges::remove_if(candidates, [&](const auto& candidate) {
                return !ToLowerUTF8(candidate.Content).StartsWith(lowerPrefix);
            });
            candidates.erase(std::begin(removed), std::end(removed));
        }

        TConfiguration Configuration;
        ILocalSyntaxAnalysis::TPtr SyntaxAnalysis;
        INameService::TPtr Names;
    };

    // FIXME(YQL-19747): unwanted dependency on a lexer implementation
    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();

        INameService::TPtr names = MakeStaticNameService(MakeDefaultNameSet(), MakeDefaultRanking());

        return MakeSqlCompletionEngine([lexers = std::move(lexers)](bool ansi) {
            return NSQLTranslationV1::MakeLexer(
                lexers, ansi, /* antlr4 = */ true,
                NSQLTranslationV1::ELexerFlavor::Pure);
        }, std::move(names));
    }

    ISqlCompletionEngine::TPtr MakeSqlCompletionEngine(
        TLexerSupplier lexer,
        INameService::TPtr names,
        ISqlCompletionEngine::TConfiguration configuration) {
        return ISqlCompletionEngine::TPtr(
            new TSqlCompletionEngine(lexer, std::move(names), std::move(configuration)));
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
    }
}

template <>
void Out<NSQLComplete::TCandidate>(IOutputStream& out, const NSQLComplete::TCandidate& candidate) {
    out << "{" << candidate.Kind << ", \"" << candidate.Content << "\"}";
}

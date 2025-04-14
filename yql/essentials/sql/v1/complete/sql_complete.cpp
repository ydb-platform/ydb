#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/text/word.h>
#include <yql/essentials/sql/v1/complete/name/static/name_service.h>
#include <yql/essentials/sql/v1/complete/syntax/local.h>
#include <yql/essentials/sql/v1/complete/syntax/format.h>

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

            return {
                .CompletedToken = std::move(completedToken),
                .Candidates = GetCanidates(std::move(context), completedToken),
            };
        }

    private:
        TCompletedToken GetCompletedToken(TStringBuf prefix) {
            return {
                .Content = LastWord(prefix),
                .SourcePosition = LastWordIndex(prefix),
            };
        }

        TVector<TCandidate> GetCanidates(TLocalSyntaxContext context, const TCompletedToken& prefix) {
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
                return {};
            }

            // User should prepare a robust INameService
            TNameResponse response = Names->Lookup(std::move(request)).ExtractValueSync();

            return Convert(std::move(response.RankedNames), std::move(context.Keywords));
        }

        TVector<TCandidate> Convert(TVector<TGenericName> names, TLocalSyntaxContext::TKeywords keywords) {
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
        case NSQLComplete::ECandidateKind::HintName:
            out << "HintName";
            break;
    }
}

template <>
void Out<NSQLComplete::TCandidate>(IOutputStream& out, const NSQLComplete::TCandidate& candidate) {
    out << "{" << candidate.Kind << ", \"" << candidate.Content << "\"}";
}

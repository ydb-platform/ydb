#include "yql_suggest.h"

#include "yql_syntax.h"

#include <contrib/libs/antlr4_cpp_runtime/src/ANTLRInputStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/BufferedTokenStream.h>
#include <contrib/libs/antlr4-c3/src/CodeCompletionCore.hpp>

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

namespace NYdb {
    namespace NConsoleClient {

        class AntlrPipeline {
        public:
            using TPtr = THolder<AntlrPipeline>;

            virtual antlr4::Lexer& GetLexer() = 0;
            virtual antlr4::BufferedTokenStream& GetTokens() = 0;
            virtual antlr4::Parser& GetParser() = 0;
            virtual ~AntlrPipeline() = default;
        };

        template <class YQLLexer, class YQLParser>
        class YQLAntlrPipeline final: public AntlrPipeline {
        public:
            explicit YQLAntlrPipeline(const TString& input)
                : Chars(input)
                , Lexer(&Chars)
                , Tokens(&Lexer)
                , Parser(&Tokens)
            {
            }

            antlr4::Lexer& GetLexer() override {
                return Lexer;
            }

            antlr4::BufferedTokenStream& GetTokens() override {
                return Tokens;
            }

            antlr4::Parser& GetParser() override {
                return Parser;
            }

        private:
            antlr4::ANTLRInputStream Chars;
            YQLLexer Lexer;
            antlr4::BufferedTokenStream Tokens;
            YQLParser Parser;
        };

        using CppPipeline = YQLAntlrPipeline<
            NALPDefaultAntlr4::SQLv1Antlr4Lexer,
            NALPDefaultAntlr4::SQLv1Antlr4Parser>;

        using AnsiPipeline = YQLAntlrPipeline<
            NALPAnsiAntlr4::SQLv1Antlr4Lexer,
            NALPAnsiAntlr4::SQLv1Antlr4Parser>;

        AntlrPipeline::TPtr SuitablePipeline(const TString& queryUtf8) {
            if (IsAnsiQuery(queryUtf8)) {
                return MakeHolder<AnsiPipeline>(queryUtf8);
            }
            return MakeHolder<CppPipeline>(queryUtf8);
        }

        size_t LastWordIndex(TStringBuf text) {
            auto length = static_cast<int64_t>(text.size());
            for (int64_t i = length - 1; 0 <= i; --i) {
                if (IsWhitespace(text[i])) {
                    return i + 1;
                }
            }
            return 0;
        }

        TString LastWord(TStringBuf text) {
            return TString(text.substr(LastWordIndex(text)));
        }

        Completions YQLSuggestionEngine::Suggest(TStringBuf queryUtf8) {
            auto pipeline = SuitablePipeline(TString(queryUtf8));
            auto& lexer = pipeline->GetLexer();
            auto& tokens = pipeline->GetTokens();
            c3::CodeCompletionCore engine(&pipeline->GetParser());

            size_t caretTokenIndex = (2 <= tokens.size()) ? tokens.size() - 2 : tokens.size() - 1;
            c3::CandidatesCollection candidates = engine.collectCandidates(caretTokenIndex);

            auto lastWord = ToLowerUTF8(LastWord(queryUtf8));
            auto isSuitable = [&](TStringBuf candidate) {
                return ToLowerUTF8(candidate).StartsWith(lastWord);
            };

            auto toString = [&](size_t token) -> TString {
                const auto& vocabulary = lexer.getVocabulary();
                auto display = vocabulary.getDisplayName(token);
                if (display.starts_with('\'')) {
                    assert(display.ends_with('\''));
                    display.erase(std::begin(display));
                    display.erase(std::prev(std::end(display)));
                }
                return display;
            };

            TVector<TString> words;
            for (const auto& [token, follow] : candidates.tokens) {
                auto candidate = toString(token);
                if (isSuitable(candidate)) {
                    words.emplace_back(std::move(candidate));
                }
            }
            for (auto& word : words) {
                word += ' ';
            }

            Completions result;
            for (auto& word : words) {
                result.emplace_back(std::move(word));
            }
            return result;
        }
    } // namespace NConsoleClient
} // namespace NYdb

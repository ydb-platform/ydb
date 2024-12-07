#include "yql_suggest.h"

#include "yql_syntax.h"

#include <contrib/libs/antlr4_cpp_runtime/src/ANTLRInputStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/BufferedTokenStream.h>
#include <contrib/libs/antlr4-c3/src/CodeCompletionCore.hpp>

#include <yql/essentials/sql/v1/format/sql_format.h>

#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#define RULE(mode, name) NALP##mode##Antlr4::SQLv1Antlr4Parser::Rule##name

#define STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(name) \
    static_assert(RULE(Default, name) == RULE(Ansi, name))

namespace NYdb {
    namespace NConsoleClient {

        const std::initializer_list<size_t> YQLKeywordRules = {
            RULE(Default, Keyword),
            RULE(Default, Keyword_expr_uncompat),
            RULE(Default, Keyword_table_uncompat),
            RULE(Default, Keyword_select_uncompat),
            RULE(Default, Keyword_alter_uncompat),
            RULE(Default, Keyword_in_uncompat),
            RULE(Default, Keyword_window_uncompat),
            RULE(Default, Keyword_hint_uncompat),
            RULE(Default, Keyword_as_compat),
            RULE(Default, Keyword_compat),
        };

        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_expr_uncompat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_table_uncompat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_select_uncompat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_alter_uncompat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_in_uncompat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_window_uncompat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_hint_uncompat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_as_compat);
        STATIC_ASSERT_RULE_ID_MODE_INDEPENDENT(Keyword_compat);

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

        YQLSuggestionEngine::YQLSuggestionEngine()
        {
            auto keywords = NSQLFormat::GetKeywords();

            TMap<TString, AntlrPipeline::TPtr> pipelines;
            pipelines.emplace("cpp", MakeHolder<CppPipeline>(""));
            pipelines.emplace("ansi", MakeHolder<AnsiPipeline>(""));

            for (auto& [mode, pipeline] : pipelines) {
                auto& vocabulary = pipeline->GetLexer().getVocabulary();
                for (size_t type = 1; type <= vocabulary.getMaxTokenType(); ++type) {
                    if (!keywords.contains(vocabulary.getSymbolicName(type))) {
                        if (mode == "cpp") {
                            CppIgnoredTokens.insert(type);
                        } else if (mode == "ansi") {
                            AnsiIgnoredTokens.insert(type);
                        }
                    }
                }
            }
        }

        Completions YQLSuggestionEngine::Suggest(TStringBuf queryUtf8) {
            auto pipeline = SuitablePipeline(TString(queryUtf8));
            auto& lexer = pipeline->GetLexer();
            auto& tokens = pipeline->GetTokens();
            c3::CodeCompletionCore engine(&pipeline->GetParser());

            if (IsAnsiQuery(TString(queryUtf8))) {
                engine.ignoredTokens = AnsiIgnoredTokens;
            } else {
                engine.ignoredTokens = CppIgnoredTokens;
            }

            engine.preferredRules = YQLKeywordRules;

            tokens.fill();
            size_t caretTokenIndex = (2 <= tokens.size()) ? tokens.size() - 2 : tokens.size() - 1;
            c3::CandidatesCollection candidates = engine.collectCandidates(caretTokenIndex);

            std::vector<size_t> justKeywords;
            for (const auto& [token, _] : candidates.tokens) {
                const auto& rules = candidates.rules[token].ruleList;
                for (const auto keywordRule : YQLKeywordRules) {
                    if (std::ranges::find(rules, keywordRule) != std::end(rules)) {
                        continue;
                    }
                }
                justKeywords.emplace_back(token);
            }

            const auto lastWord = ToLowerUTF8(LastWord(queryUtf8));
            const auto isSuitable = [&](TStringBuf candidate) {
                return ToLowerUTF8(candidate).StartsWith(lastWord);
            };

            const auto toString = [&](size_t token) -> TString {
                const auto& vocabulary = lexer.getVocabulary();
                auto display = vocabulary.getDisplayName(token);
                assert(!display.starts_with('\''));
                return display;
            };

            TVector<TString> words;
            for (const auto token : justKeywords) {
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

#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>
#include <yql/essentials/parser/lexer_common/lexer.h>

#include <util/generic/fwd.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>

#include <regex>

namespace NYdb {
    namespace NConsoleClient {
        using NSQLTranslation::ILexer;
        using NSQLTranslation::TParsedToken;
        using NSQLTranslation::TParsedTokenList;

        class YQLHighlight final {
        public:
            using Color = replxx::Replxx::Color;

            // Colors are provided as for a UTF32 string
            using Colors = replxx::Replxx::colors_t;

            struct ColorSchema {
                Color keyword;
                Color operation;
                struct {
                    Color function;
                    Color type;
                    Color variable;
                    Color quoted;
                } identifier;
                Color string;
                Color number;
                Color comment;
                Color unknown;

                static ColorSchema Monaco();
                static ColorSchema Debug();
            };

        public:
            explicit YQLHighlight(ColorSchema color);

        public:
            void Apply(TStringBuf queryUtf8, Colors& colors);

        private:
            TParsedTokenList Tokenize(const TString& queryUtf8);
            ILexer::TPtr& SuitableLexer(const TString& queryUtf8);
            YQLHighlight::Color ColorOf(const TParsedToken& token, size_t index);
            bool IsKeyword(const TParsedToken& token) const;
            bool IsOperation(const TParsedToken& token) const;
            bool IsFunctionIdentifier(const TParsedToken& token, size_t index);
            bool IsTypeIdentifier(const TParsedToken& token) const;
            bool IsVariableIdentifier(const TParsedToken& token) const;
            bool IsQuotedIdentifier(const TParsedToken& token) const;
            bool IsString(const TParsedToken& token) const;
            bool IsNumber(const TParsedToken& token) const;
            bool IsComment(const TParsedToken& token) const;

        private:
            ColorSchema Coloring;
            std::regex BuiltinFunctionRegex;
            std::regex TypeRegex;

            ILexer::TPtr CppLexer;
            ILexer::TPtr ANSILexer;

            TParsedTokenList Tokens;
        };

    } // namespace NConsoleClient
} // namespace NYdb

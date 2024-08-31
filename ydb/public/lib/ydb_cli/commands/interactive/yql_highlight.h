#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <contrib/libs/antlr4_cpp_runtime/src/Token.h>
#include <contrib/libs/antlr4_cpp_runtime/src/BufferedTokenStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/ANTLRInputStream.h>

#include <ydb/public/lib/ydb_cli/commands/interactive/antlr/YQLLexer.h>

#include <regex>

namespace NYdb {
    namespace NConsoleClient {

        class YQLHighlight final {
        public:
            using Color = replxx::Replxx::Color;
            using Colors = replxx::Replxx::colors_t;

            struct ColorSchema {
                Color keyword;
                Color operation;
                struct {
                    Color function;
                    Color simple;
                    Color quoted;
                } identifier;
                Color string;
                Color number;
                Color unknown;

                static ColorSchema Monaco();
            };

        public:
            explicit YQLHighlight(ColorSchema color);

        public:
            void Apply(std::string_view query, Colors& colors);

        private:
            void Reset(std::string_view query);

            YQLHighlight::Color ColorOf(const antlr4::Token* token);
            bool IsKeyword(const antlr4::Token* token) const;
            bool IsOperation(const antlr4::Token* token) const;
            bool IsFunctionIdentifier(const antlr4::Token* token);
            bool IsSimpleIdentifier(const antlr4::Token* token) const;
            bool IsQuotedIdentifier(const antlr4::Token* token) const;
            bool IsString(const antlr4::Token* token) const;
            bool IsNumber(const antlr4::Token* token) const;

        private:
            ColorSchema Coloring;
            std::regex BuiltinFunctionRegex;

            antlr4::ANTLRInputStream Chars;
            YQLLexer Lexer;
            antlr4::BufferedTokenStream Tokens;
        };

    }
}

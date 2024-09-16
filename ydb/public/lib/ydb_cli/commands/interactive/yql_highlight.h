#pragma once

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <contrib/libs/antlr4_cpp_runtime/src/Token.h>
#include <contrib/libs/antlr4_cpp_runtime/src/BufferedTokenStream.h>
#include <contrib/libs/antlr4_cpp_runtime/src/ANTLRInputStream.h>

#include <ydb/library/yql/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>

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
                    Color type;
                    Color variable;
                    Color quoted;
                } identifier;
                Color string;
                Color number;
                Color unknown;

                static ColorSchema Monaco();
                static ColorSchema Debug();
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
            bool IsTypeIdentifier(const antlr4::Token* token) const;
            bool IsVariableIdentifier(const antlr4::Token* token) const;
            bool IsQuotedIdentifier(const antlr4::Token* token) const;
            bool IsString(const antlr4::Token* token) const;
            bool IsNumber(const antlr4::Token* token) const;

        private:
            ColorSchema Coloring;
            std::regex BuiltinFunctionRegex;
            std::regex TypeRegex;

            antlr4::ANTLRInputStream Chars;
            NALPDefaultAntlr4::SQLv1Antlr4Lexer Lexer;
            antlr4::BufferedTokenStream Tokens;
        };

    }
}

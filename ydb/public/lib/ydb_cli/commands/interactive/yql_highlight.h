#pragma once

#include <contrib/libs/antlr4_cpp_runtime/src/Token.h>
#include <contrib/restricted/patched/replxx/include/replxx.hxx>

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
            YQLHighlight::Color ColorOf(const antlr4::Token* token) const;
            bool IsKeyword(const antlr4::Token* token) const;
            bool IsOperation(const antlr4::Token* token) const;
            bool IsFunctionIdentifier(const antlr4::Token* token) const;
            bool IsSimpleIdentifier(const antlr4::Token* token) const;
            bool IsQuotedIdentifier(const antlr4::Token* token) const;
            bool IsString(const antlr4::Token* token) const;
            bool IsNumber(const antlr4::Token* token) const;

        private:
            ColorSchema Coloring;
        };

    }
}

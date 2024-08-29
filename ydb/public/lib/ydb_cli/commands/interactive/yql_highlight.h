#pragma once

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
                Color simple_identifier;
                Color quoted_identifier;
                Color string;
                Color number;
                Color unknown;

                static ColorSchema Default();
            };

        public:
            explicit YQLHighlight(ColorSchema color);

        public:
            void Apply(std::string_view query, Colors& colors);

        private:
            ColorSchema Coloring;
        };

    }
}

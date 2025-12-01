#include "schema.h"
#include <util/stream/output.h>

namespace NYdb::NConsoleClient {

namespace {

    TColorSchema NoColor() {
        return {
            .keyword = TColor::DEFAULT,
            .operation = TColor::DEFAULT,
            .identifier = {
                .function = TColor::DEFAULT,
                .type = TColor::DEFAULT,
                .variable = TColor::DEFAULT,
                .quoted = TColor::DEFAULT,
            },
            .string = TColor::DEFAULT,
            .number = TColor::DEFAULT,
            .comment = TColor::DEFAULT,
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema Monaco() {
        using replxx::color::rgb666;

        return {
            .keyword = TColor::BLUE,
            .operation = rgb666(3, 3, 3),
            .identifier = {
                .function = rgb666(4, 1, 5),
                .type = rgb666(2, 3, 2),
                .variable = TColor::DEFAULT,
                .quoted = rgb666(1, 3, 3),
            },
            .string = rgb666(3, 0, 0),
            .number = TColor::BRIGHTGREEN,
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema Debug() {
        using replxx::color::rgb666;

        return {
            .keyword = TColor::BLUE,
            .operation = TColor::GRAY,
            .identifier = {
                .function = TColor::MAGENTA,
                .type = TColor::YELLOW,
                .variable = TColor::RED,
                .quoted = TColor::CYAN,
            },
            .string = TColor::GREEN,
            .number = TColor::BRIGHTGREEN,
            .comment = rgb666(2, 2, 2),
            .unknown = TColor::DEFAULT,
        };
    }

    TColorSchema Default() {
        return Monaco();
    }
}

TColorSchema GetColorSchema(const std::string& name) {
    if (name.empty()) {
        return Default();
    }
    if (name == "no_color") {
        return NoColor();
    }
    if (name == "monaco") {
        return Monaco();
    }
    if (name == "debug") {
        return Debug();
    }
    Cerr << "Unknown color schema: " << name << Endl;
    return Default();
}
} // namespace NYdb::NConsoleClient

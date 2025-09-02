#include "schema.h"

namespace NYdb::NConsoleClient {

    TColorSchema TColorSchema::Monaco() {
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

    TColorSchema TColorSchema::Debug() {
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

} // namespace NYdb::NConsoleClient

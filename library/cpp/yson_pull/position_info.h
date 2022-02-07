#pragma once

#include <util/generic/maybe.h>
#include <util/system/types.h>

namespace NYsonPull {
    struct TPositionInfo {
        TMaybe<ui64> Offset;
        TMaybe<ui64> Line;
        TMaybe<ui64> Column;

        TPositionInfo() = default;
        TPositionInfo(
            TMaybe<ui64> offset_,
            TMaybe<ui64> line_ = Nothing(),
            TMaybe<ui64> column_ = Nothing())
            : Offset{offset_}
            , Line{line_}
            , Column{column_} {
        }
    };

}

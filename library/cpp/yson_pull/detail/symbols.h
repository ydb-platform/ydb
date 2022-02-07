#pragma once

#include <util/generic/strbuf.h>
#include <util/system/types.h>

namespace NYsonPull {
    namespace NDetail {
        namespace NSymbol {
#define SYM(name, value) constexpr ui8 name = value

            //! Indicates the beginning of a list.
            SYM(begin_list, '[');
            //! Indicates the end of a list.
            SYM(end_list, ']');

            //! Indicates the beginning of a map.
            SYM(begin_map, '{');
            //! Indicates the end of a map.
            SYM(end_map, '}');

            //! Indicates the beginning of an attribute map.
            SYM(begin_attributes, '<');
            //! Indicates the end of an attribute map.
            SYM(end_attributes, '>');

            //! Separates items in lists and pairs in maps or attribute maps.
            SYM(item_separator, ';');
            //! Separates keys from values in maps and attribute maps.
            SYM(key_value_separator, '=');

            //! Indicates an entity.
            SYM(entity, '#');
            //! Indicates end of stream.
            SYM(eof, '\0');

            //! Marks the beginning of a binary string literal.
            SYM(string_marker, '\x01');
            //! Marks the beginning of a binary int64 literal.
            SYM(int64_marker, '\x02');
            //! Marks the beginning of a binary uint64 literal.
            SYM(uint64_marker, '\x06');
            //! Marks the beginning of a binary double literal.
            SYM(double_marker, '\x03');
            //! Marks a binary `false' boolean value.
            SYM(false_marker, '\x04');
            //! Marks a binary `true' boolean value.
            SYM(true_marker, '\x05');

            //! Text string quote symbol
            SYM(quote, '"');

#undef SYM
        }
    }
}

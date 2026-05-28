#pragma once

// Internal implementation for TRowRange::Get. Not a supported extension point.
// Included from rows.h inside NYdb::Dev after TRowParser is complete.

namespace NRowRangesDetail {

template <class T, class = void>
struct TRowColumnGetter {
    static T Get(TRowParser& row, const std::string& name) {
        return TValueParserGetter<T>::Get(row.ColumnParser(name));
    }
};

template <>
struct TRowColumnGetter<TValue> {
    static TValue Get(TRowParser& row, const std::string& name) {
        return row.GetValue(name);
    }
};

} // namespace NRowRangesDetail

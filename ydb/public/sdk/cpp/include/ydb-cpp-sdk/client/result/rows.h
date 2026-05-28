#pragma once

#include "result.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/fwd.h>

#include <array>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

namespace NYdb::inline Dev {

class TRowIterEnd {};

class TRowParser;

class TRowParserHolder;

template <class... Args>
class TRowColumns;

class TRowIterator {
    friend class TRowRange;
public:
    class TImpl;

    TRowIterator(const TRowIterator&) = delete;
    TRowIterator(TRowIterator&&) noexcept;
    ~TRowIterator();
    TRowIterator& operator=(const TRowIterator&) = delete;
    TRowIterator& operator=(TRowIterator&&) noexcept;

    bool operator==(const TRowIterator& other) const;
    bool operator!=(const TRowIterator& other) const;
    bool operator==(const TRowIterEnd& other) const;
    bool operator!=(const TRowIterEnd& other) const;

    TRowParser& operator*() const;
    TRowParser* operator->() const;

    TRowIterator& operator++();

private:
    explicit TRowIterator(std::unique_ptr<TImpl> impl);

    std::unique_ptr<TImpl> Impl_;
};

class TRowParser {
    friend class TRowIterator::TImpl;
    friend class TRowParserHolder;
public:
    TRowParser(const TRowParser&) = delete;
    TRowParser(TRowParser&&) = delete;
    TRowParser& operator=(const TRowParser&) = delete;
    TRowParser& operator=(TRowParser&&) = delete;

    size_t ColumnsCount() const;
    size_t RowsCount() const;
    ssize_t ColumnIndex(const std::string& columnName);
    TValueParser& ColumnParser(size_t columnIndex);
    TValueParser& ColumnParser(const std::string& columnName);
    TValue GetValue(size_t columnIndex) const;
    TValue GetValue(const std::string& columnName) const;

private:
    explicit TRowParser(TResultSetParser& parser);

    TResultSetParser& Parser_;
};

//! Forward, single-pass range over rows of a single logical result set.
class TRowRange {
    friend class TRowIterator::TImpl;
public:
    explicit TRowRange(TResultSet&& resultSet);
    explicit TRowRange(NTable::TDataQueryResult&& result);

    explicit TRowRange(NQuery::TExecuteQueryIterator&& iterator);
    explicit TRowRange(NTable::TScanQueryPartIterator&& iterator);
    explicit TRowRange(NTable::TTablePartIterator&& iterator);

    TRowRange(const TRowRange&) = delete;
    TRowRange(TRowRange&&) noexcept;
    TRowRange& operator=(const TRowRange&) = delete;
    TRowRange& operator=(TRowRange&&) noexcept;
    ~TRowRange();

    TRowIterator begin();
    TRowIterEnd end();

    //! Build a typed-tuple view over this range.
    //! Args... are the column C++ types (e.g. int32_t, std::optional<std::string>);
    //! columns is any iterable of names convertible to std::string.
    //! Yields std::tuple<Args...> per row.
    template <class... Args>
    TRowColumns<Args...> Get(std::initializer_list<std::string_view> columns);

    template <class... Args, class StringIterable>
    TRowColumns<Args...> Get(const StringIterable& columns);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

namespace NRowRangesDetail {

//! Maps a C++ type to the TValueParser accessor that reads it.
//! Unspecialised T fails to compile ("incomplete type") — that is the intended
//! error for unsupported column types.
template <class T>
struct TValueParserGetter;

#define Y_DEFINE_VALUE_PARSER_GETTER(Type, Suffix) \
    template <> struct TValueParserGetter<Type> { \
        static Type Get(TValueParser& p) { return p.Get##Suffix(); } \
    }; \
    template <> struct TValueParserGetter<std::optional<Type>> { \
        static std::optional<Type> Get(TValueParser& p) { return p.GetOptional##Suffix(); } \
    }

Y_DEFINE_VALUE_PARSER_GETTER(bool,        Bool);
Y_DEFINE_VALUE_PARSER_GETTER(int8_t,      Int8);
Y_DEFINE_VALUE_PARSER_GETTER(uint8_t,     Uint8);
Y_DEFINE_VALUE_PARSER_GETTER(int16_t,     Int16);
Y_DEFINE_VALUE_PARSER_GETTER(uint16_t,    Uint16);
Y_DEFINE_VALUE_PARSER_GETTER(int32_t,     Int32);
Y_DEFINE_VALUE_PARSER_GETTER(uint32_t,    Uint32);
Y_DEFINE_VALUE_PARSER_GETTER(int64_t,     Int64);
Y_DEFINE_VALUE_PARSER_GETTER(uint64_t,    Uint64);
Y_DEFINE_VALUE_PARSER_GETTER(float,       Float);
Y_DEFINE_VALUE_PARSER_GETTER(double,      Double);
Y_DEFINE_VALUE_PARSER_GETTER(std::string, Utf8);
Y_DEFINE_VALUE_PARSER_GETTER(TUuidValue,  Uuid);

#undef Y_DEFINE_VALUE_PARSER_GETTER

//! TInstant maps to whichever date/time primitive the column actually holds:
//! Date, Datetime, or Timestamp. All three TValueParser accessors return TInstant
//! at the C++ level; only the runtime primitive type tells us which one is valid.
template <> struct TValueParserGetter<TInstant> {
    static TInstant Get(TValueParser& p) {
        switch (p.GetPrimitiveType()) {
            case EPrimitiveType::Date:      return p.GetDate();
            case EPrimitiveType::Datetime:  return p.GetDatetime();
            case EPrimitiveType::Timestamp: return p.GetTimestamp();
            default:
                throw std::runtime_error(
                    "TValueParserGetter<TInstant>: column type is not Date/Datetime/Timestamp");
        }
    }
};

template <> struct TValueParserGetter<std::optional<TInstant>> {
    static std::optional<TInstant> Get(TValueParser& p) {
        p.OpenOptional();
        std::optional<TInstant> result;
        bool unsupported = false;
        if (!p.IsNull()) {
            switch (p.GetPrimitiveType()) {
                case EPrimitiveType::Date:      result = p.GetDate();      break;
                case EPrimitiveType::Datetime:  result = p.GetDatetime();  break;
                case EPrimitiveType::Timestamp: result = p.GetTimestamp(); break;
                default: unsupported = true; break;
            }
        }
        p.CloseOptional();
        if (unsupported) {
            throw std::runtime_error(
                "TValueParserGetter<std::optional<TInstant>>: column inner type is not Date/Datetime/Timestamp");
        }
        return result;
    }
};

} // namespace NRowRangesDetail

//! Lightweight typed view over a TRowRange. Holds only a reference to
//! the underlying range and the column names; yields std::tuple<Args...> per row.
template <class... Args>
class TRowColumns {
    friend class TRowRange;
    static constexpr size_t N = sizeof...(Args);
    using TNames = std::array<std::string, N>;
public:
    class Iterator {
        friend class TRowColumns;
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = std::tuple<Args...>;
        using difference_type = std::ptrdiff_t;
        using pointer = void;
        using reference = value_type;

        Iterator(const Iterator&) = delete;
        Iterator(Iterator&&) noexcept = default;
        Iterator& operator=(const Iterator&) = delete;
        Iterator& operator=(Iterator&&) noexcept = default;

        value_type operator*() const {
            return MakeTuple(std::index_sequence_for<Args...>{});
        }

        Iterator& operator++() {
            ++Inner_;
            return *this;
        }

        void operator++(int) {
            ++(*this);
        }

        bool operator==(const TRowIterEnd& end) const { return Inner_ == end; }
        bool operator!=(const TRowIterEnd& end) const { return Inner_ != end; }

    private:
        Iterator(TRowIterator&& inner, const TNames& names)
            : Inner_(std::move(inner))
            , Names_(&names)
        {}

        template <size_t... Is>
        value_type MakeTuple(std::index_sequence<Is...>) const {
            TRowParser& row = *Inner_;
            return value_type{
                NRowRangesDetail::TValueParserGetter<Args>::Get(row.ColumnParser((*Names_)[Is]))...
            };
        }

        TRowIterator Inner_;
        const TNames* Names_;
    };

    TRowColumns(const TRowColumns&) = delete;
    TRowColumns(TRowColumns&&) noexcept = default;
    TRowColumns& operator=(const TRowColumns&) = delete;
    TRowColumns& operator=(TRowColumns&&) noexcept = default;

    Iterator begin() {
        return Iterator(Range_.begin(), Names_);
    }

    TRowIterEnd end() const noexcept {
        return TRowIterEnd{};
    }

private:
    TRowColumns(TRowRange& range, TNames&& names)
        : Range_(range)
        , Names_(std::move(names))
    {}

    TRowRange& Range_;
    TNames Names_;
};

namespace NRowRangesDetail {

template <size_t N, class It>
std::array<std::string, N> BuildColumnNames(It first, It last) {
    std::array<std::string, N> names;
    size_t i = 0;
    for (auto it = first; it != last; ++it) {
        if (i >= N) {
            throw std::invalid_argument(
                "TRowRange::Get: too many column names for the requested types");
        }
        names[i++] = std::string(*it);
    }
    if (i != N) {
        throw std::invalid_argument(
            "TRowRange::Get: not enough column names for the requested types");
    }
    return names;
}

} // namespace NRowRangesDetail

template <class... Args>
TRowColumns<Args...> TRowRange::Get(std::initializer_list<std::string_view> columns) {
    return TRowColumns<Args...>(
        *this,
        NRowRangesDetail::BuildColumnNames<sizeof...(Args)>(columns.begin(), columns.end()));
}

template <class... Args, class StringIterable>
TRowColumns<Args...> TRowRange::Get(const StringIterable& columns) {
    return TRowColumns<Args...>(
        *this,
        NRowRangesDetail::BuildColumnNames<sizeof...(Args)>(std::begin(columns), std::end(columns)));
}

} // namespace NYdb::inline Dev

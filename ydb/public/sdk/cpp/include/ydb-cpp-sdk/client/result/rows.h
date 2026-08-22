#pragma once

#include "result.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/fwd.h>

#include <array>
#include <cstddef>
#include <initializer_list>
#include <iterator>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>

#include "detail/rows_parser_getter.h"

namespace NYdb::inline Dev {

class TRowIterEnd {};

class TRowParser;

class TRowParserHolder;

template <class... Args>
class TRowColumns;

class TRowIterator;

class TRowRange;

class TRowParser {
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

} // namespace NYdb::inline Dev

#include "detail/rows_column_getter.h"

namespace NYdb::inline Dev {

//! Move-only, single-pass input iterator over rows.
//! Valid while shared range state is alive. Not copyable and not multipass:
//! do not expect independent copies, re-traversal, or classic STL algorithms
//! that require copyable input iterators. Advertises input_iterator_tag for
//! ranges-style sentinel iteration, not classic copyable input iterators.
class TRowIterator {
    friend class TRowRange;
public:
    class TImpl;

    using iterator_category = std::input_iterator_tag;
    using value_type = TRowParser;
    using difference_type = std::ptrdiff_t;
    using pointer = TRowParser*;
    using reference = TRowParser&;

    TRowIterator(const TRowIterator&) = delete;
    TRowIterator(TRowIterator&&) noexcept;
    ~TRowIterator();
    TRowIterator& operator=(const TRowIterator&) = delete;
    TRowIterator& operator=(TRowIterator&&) noexcept;

    bool operator==(const TRowIterEnd& end) const noexcept;
    bool operator!=(const TRowIterEnd& end) const noexcept;

    reference operator*() const;
    pointer operator->() const;

    TRowIterator& operator++();
    void operator++(int) {
        ++(*this);
    }

private:
    explicit TRowIterator(std::unique_ptr<TImpl> impl);

    std::unique_ptr<TImpl> Impl_;
};

inline bool operator==(const TRowIterEnd& end, const TRowIterator& it) noexcept {
    return it == end;
}

inline bool operator!=(const TRowIterEnd& end, const TRowIterator& it) noexcept {
    return it != end;
}

//! Forward, single-pass range over rows of a single logical result set.
class TRowRange {
    friend class TRowIterator::TImpl;
    template <class... Args>
    friend class TRowColumns;
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
    TRowIterEnd end() const noexcept;

    //! Build a typed-tuple view over this range.
    //! Args... are the column C++ types (e.g. int32_t, std::optional<std::string>);
    //! columns is any iterable of names convertible to std::string.
    //! Yields std::tuple<Args...> per row.
    template <class... Args>
    TRowColumns<Args...> Get(std::initializer_list<std::string_view> columns);

    template <class... Args, class StringIterable>
    TRowColumns<Args...> Get(const StringIterable& columns);

    class TImpl;

private:
    static TRowIterator BeginIterator(const std::shared_ptr<TImpl>& state);

    std::shared_ptr<TImpl> Impl_;
};

//! Lightweight typed view over a TRowRange. Holds shared range state and column
//! names; yields std::tuple<Args...> per row.
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
                NRowRangesDetail::TRowColumnGetter<Args>::Get(row, (*Names_)[Is])...
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
        return Iterator(TRowRange::BeginIterator(State_), Names_);
    }

    TRowIterEnd end() const noexcept {
        return TRowIterEnd{};
    }

private:
    TRowColumns(std::shared_ptr<TRowRange::TImpl> state, TNames&& names)
        : State_(std::move(state))
        , Names_(std::move(names))
    {}

    std::shared_ptr<TRowRange::TImpl> State_;
    TNames Names_;
};

template <class... Args>
TRowColumns<Args...> TRowRange::Get(std::initializer_list<std::string_view> columns) {
    return TRowColumns<Args...>(
        Impl_,
        NRowRangesDetail::BuildColumnNames<sizeof...(Args)>(columns.begin(), columns.end()));
}

template <class... Args, class StringIterable>
TRowColumns<Args...> TRowRange::Get(const StringIterable& columns) {
    return TRowColumns<Args...>(
        Impl_,
        NRowRangesDetail::BuildColumnNames<sizeof...(Args)>(std::begin(columns), std::end(columns)));
}

} // namespace NYdb::inline Dev

#pragma once

#include "result.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/fwd.h>

#include <memory>

namespace NYdb::inline Dev {

class TResultIterEnd {};

class TResultRowParser;

class TResultIterator {
    friend class TResultSetRange;
public:
    class TImpl;

    TResultIterator(const TResultIterator&) = delete;
    TResultIterator(TResultIterator&&) noexcept;
    ~TResultIterator();
    TResultIterator& operator=(const TResultIterator&) = delete;
    TResultIterator& operator=(TResultIterator&&) noexcept;

    bool operator==(const TResultIterator& other) const;
    bool operator!=(const TResultIterator& other) const;
    bool operator==(const TResultIterEnd& other) const;
    bool operator!=(const TResultIterEnd& other) const;

    TResultRowParser& operator*() const;
    TResultRowParser* operator->() const;

    TResultIterator& operator++();

private:
    explicit TResultIterator(std::unique_ptr<TImpl> impl);

    std::unique_ptr<TImpl> Impl_;
};

class TResultRowParser {
    friend class TResultIterator::TImpl;
public:
    TResultRowParser(const TResultRowParser&) = delete;
    TResultRowParser(TResultRowParser&&) = delete;
    TResultRowParser& operator=(const TResultRowParser&) = delete;
    TResultRowParser& operator=(TResultRowParser&&) = delete;

    size_t ColumnsCount() const;
    size_t RowsCount() const;
    ssize_t ColumnIndex(const std::string& columnName);
    TValueParser& ColumnParser(size_t columnIndex);
    TValueParser& ColumnParser(const std::string& columnName);
    TValue GetValue(size_t columnIndex) const;
    TValue GetValue(const std::string& columnName) const;

private:
    explicit TResultRowParser(TResultSetParser& parser);

    TResultSetParser& Parser_;
};

//! Forward, single-pass range over rows of one or more TResultSet's.
class TResultSetRange {
    friend class TResultIterator::TImpl;
public:
    explicit TResultSetRange(TResultSet&& resultSet);
    explicit TResultSetRange(NTable::TDataQueryResult&& result);

    explicit TResultSetRange(NQuery::TExecuteQueryIterator&& iterator);
    explicit TResultSetRange(NTable::TScanQueryPartIterator&& iterator);
    explicit TResultSetRange(NTable::TTablePartIterator&& iterator);

    TResultSetRange(const TResultSetRange&) = delete;
    TResultSetRange(TResultSetRange&&) noexcept;
    TResultSetRange& operator=(const TResultSetRange&) = delete;
    TResultSetRange& operator=(TResultSetRange&&) noexcept;
    ~TResultSetRange();

    TResultIterator begin();
    TResultIterEnd end();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

} // namespace NYdb::inline Dev

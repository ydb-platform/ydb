#pragma once

#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace Ydb {
    class ResultSet;
}

namespace NYdb {

class TProtoAccessor;

struct TColumn {
    TString Name;
    TType Type;

    TColumn(const TString& name, const TType& type)
        : Name(name)
        , Type(type) {}

    TString ToString() const;
    void Out(IOutputStream& o) const;
};

bool operator==(const TColumn& col1, const TColumn& col2);
bool operator!=(const TColumn& col1, const TColumn& col2);

//! Collection of rows, represents result of query or part of the result in case of stream operations
class TResultSet {
    friend class TResultSetParser;
    friend class NYdb::TProtoAccessor;
public:
    TResultSet(const Ydb::ResultSet& proto);
    TResultSet(Ydb::ResultSet&& proto);

    //! Returns number of columns
    size_t ColumnsCount() const;

    //! Returns number of rows in result set (which is partial in case of stream operations)
    size_t RowsCount() const;

    //! Returns true if result set was truncated
    bool Truncated() const;

    //! Returns meta information (name, type) for columns
    const TVector<TColumn>& GetColumnsMeta() const;

private:
    const Ydb::ResultSet& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

//! Note: TResultSetParser - mutable object, iteration thougth it changes internal state
class TResultSetParser : public TMoveOnly {
public:
    TResultSetParser(TResultSetParser&&);
    TResultSetParser(const TResultSet& resultSet);

    ~TResultSetParser();

    //! Returns number of columns
    size_t ColumnsCount() const;

    //! Returns number of rows
    size_t RowsCount() const;

    //! Set iterator to the next result row.
    //! On success TryNextRow will reset all column parsers to the values in next row.
    //! Column parsers are invalid before the first TryNextRow call.
    bool TryNextRow();

    //! Returns index for column with specified name.
    //! If there is no column with such name, then -1 is returned.
    ssize_t ColumnIndex(const TString& columnName);

    //! Returns column value parser for column with specified index.
    //! State of the parser is preserved until next TryNextRow call.
    TValueParser& ColumnParser(size_t columnIndex);

    //! Returns column value parser for column with specified name.
    //! State of the parser is preserved until next TryNextRow call.
    TValueParser& ColumnParser(const TString& columnName);

    //! Returns TValue for column with specified index.
    //! TValue will have copy of coresponding data so this method
    //! is less effective compare with
    //! direct TValueParser constructed by ColumnParser call
    TValue GetValue(size_t columnIndex) const;

    //! Returns TValue for column with specified name.
    //! TValue will have copy of coresponding data so this method
    //! is less effective compare with
    //! direct TValueParser constructed by ColumnParser call
    TValue GetValue(const TString& columnName) const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

using TResultSets = TVector<TResultSet>;

} // namespace NYdb

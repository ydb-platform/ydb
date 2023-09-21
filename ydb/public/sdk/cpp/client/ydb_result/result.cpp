#include "result.h"

#include <ydb/public/sdk/cpp/client/ydb_types/fatal_error_handlers/handlers.h>

#include <ydb/public/api/protos/ydb_common.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/map.h>
#include <util/string/builder.h>

#include <google/protobuf/text_format.h>

namespace NYdb {

TString TColumn::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TColumn::Out(IOutputStream& o) const {
    o << "{ name: \"" << Name << "\""
      << ", type: " << Type
      << " }";
}

bool operator==(const TColumn& col1, const TColumn& col2) {
    return col1.Name == col2.Name && TypesEqual(col1.Type, col2.Type);
}

bool operator!=(const TColumn& col1, const TColumn& col2) {
    return !(col1 == col2);
}

class TResultSet::TImpl {
public:
    TImpl(const Ydb::ResultSet& proto)
        : ProtoResultSet_(proto)
    {
        Init();
    }

    TImpl(Ydb::ResultSet&& proto)
        : ProtoResultSet_(std::move(proto))
    {
        Init();
    }

    void Init() {
        ColumnsMeta_.reserve(ProtoResultSet_.columns_size());
        for (auto& meta : ProtoResultSet_.columns()) {
            ColumnsMeta_.push_back(TColumn(meta.name(), TType(meta.type())));
        }
    }

public:
    const Ydb::ResultSet ProtoResultSet_;
    TVector<TColumn> ColumnsMeta_;
};

////////////////////////////////////////////////////////////////////////////////

TResultSet::TResultSet(const Ydb::ResultSet& proto)
    : Impl_(new TResultSet::TImpl(proto)) {}

TResultSet::TResultSet(Ydb::ResultSet&& proto)
    : Impl_(new TResultSet::TImpl(std::move(proto))) {}

size_t TResultSet::ColumnsCount() const {
    return Impl_->ColumnsMeta_.size();
}

size_t TResultSet::RowsCount() const {
    return Impl_->ProtoResultSet_.rows_size();
}

bool TResultSet::Truncated() const {
    return Impl_->ProtoResultSet_.truncated();
}

const TVector<TColumn>& TResultSet::GetColumnsMeta() const {
    return Impl_->ColumnsMeta_;
}

const Ydb::ResultSet& TResultSet::GetProto() const {
    return Impl_->ProtoResultSet_;
}

////////////////////////////////////////////////////////////////////////////////

class TResultSetParser::TImpl {
public:
    TImpl(const TResultSet& resultSet)
        : ResultSet_(resultSet)
    {
        ColumnParsers.reserve(resultSet.ColumnsCount());

        auto& columnsMeta = resultSet.GetColumnsMeta();
        for (size_t i = 0; i < columnsMeta.size(); ++i) {
            auto& column = columnsMeta[i];
            ColumnIndexMap[column.Name] = i;
            ColumnParsers.emplace_back<TValueParser>(column.Type);
        }
    }

    size_t ColumnsCount() const {
        return ResultSet_.ColumnsCount();
    }

    size_t RowsCount() const {
        return ResultSet_.RowsCount();
    }

    bool TryNextRow() {
        if (RowIndex_ == ResultSet_.RowsCount()) {
            return false;
        }

        auto& row = ResultSet_.GetProto().rows()[RowIndex_];

        if (static_cast<size_t>(row.items_size()) != ColumnsCount()) {
            FatalError(TStringBuilder() << "Corrupted data: row " << RowIndex_ << " contains " << row.items_size() << " column(s), but metadata contains " << ColumnsCount() << " column(s)");
        }

        for (size_t i = 0; i < ColumnsCount(); ++i) {
            ColumnParsers[i].Reset(row.items(i));
        }

        RowIndex_++;
        return true;
    }

    ssize_t ColumnIndex(const TString& columnName) {
        auto idx = ColumnIndexMap.FindPtr(columnName);
        return idx ? static_cast<ssize_t>(*idx) : -1;
    }

    TValueParser& ColumnParser(size_t columnIndex) {
        if (columnIndex >= ColumnParsers.size()) {
            FatalError(TStringBuilder() << "Column index out of bounds: " << columnIndex);
        }

        return ColumnParsers[columnIndex];
    }

    TValueParser& ColumnParser(const TString& columnName) {
        auto idx = ColumnIndexMap.FindPtr(columnName);
        if (!idx) {
            FatalError(TStringBuilder() << "Unknown column: " << columnName);
        }

        return ColumnParser(*idx);
    }

    TValue GetValue(size_t columnIndex) const {
        if (columnIndex >= ColumnParsers.size()) {
            FatalError(TStringBuilder() << "Column index out of bounds: " << columnIndex);
        }

        if (RowIndex_ == 0) {
            FatalError(TStringBuilder() << "Row position is undefined");
        }

        const auto& row = ResultSet_.GetProto().rows()[RowIndex_ - 1];
        const auto& valueType = ResultSet_.GetColumnsMeta()[columnIndex].Type;

        return TValue(valueType, row.items(columnIndex));
    }

    TValue GetValue(const TString& columnName) const {
        auto idx = ColumnIndexMap.FindPtr(columnName);
        if (!idx) {
            FatalError(TStringBuilder() << "Unknown column: " << columnName);
        }

        return GetValue(*idx);
    }

private:
    void FatalError(const TString& msg) const {
        ThrowFatalError(TStringBuilder() << "TResultSetParser: " << msg);
    }

private:
    TResultSet ResultSet_;
    TMap<TString, size_t> ColumnIndexMap;
    TVector<TValueParser> ColumnParsers;
    size_t RowIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TResultSetParser::TResultSetParser(TResultSetParser&&) = default;
TResultSetParser::~TResultSetParser() = default;

TResultSetParser::TResultSetParser(const TResultSet& resultSet)
    : Impl_(new TImpl(resultSet)) {}

size_t TResultSetParser::ColumnsCount() const {
    return Impl_->ColumnsCount();
}

size_t TResultSetParser::RowsCount() const {
    return Impl_->RowsCount();
}

bool TResultSetParser::TryNextRow() {
    return Impl_->TryNextRow();
}

ssize_t TResultSetParser::ColumnIndex(const TString& columnName) {
    return Impl_->ColumnIndex(columnName);
}

TValueParser& TResultSetParser::ColumnParser(size_t columnIndex) {
    return Impl_->ColumnParser(columnIndex);
}

TValueParser& TResultSetParser::ColumnParser(const TString& columnName) {
    return Impl_->ColumnParser(columnName);
}

TValue TResultSetParser::GetValue(size_t columnIndex) const {
    return Impl_->GetValue(columnIndex);
}

TValue TResultSetParser::GetValue(const TString& columnName) const {
    return Impl_->GetValue(columnName);
}

} // namespace NYdb

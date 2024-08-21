#pragma once
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NMetadata::NInternal {

class TTableRecord {
private:
    using TValues = std::map<TString, Ydb::Value>;
    YDB_READONLY_DEF(TValues, Values);
public:
    std::vector<Ydb::Column> SelectOwnedColumns(const std::vector<Ydb::Column>& columns) const;
    Ydb::ResultSet BuildRecordSet() const;
    ui32 GetColumnsCount() const {
        return Values.size();
    }
    TTableRecord& SetColumn(const TString& columnId, const Ydb::Value& v);
    bool CompareColumns(const TTableRecord& item, const std::vector<TString>& columnIds) const;
    bool HasColumns(const std::vector<TString>& columnIds) const;
    ui32 CountIntersectColumns(const std::vector<TString>& columnIds) const;
    bool SameColumns(const TTableRecord& item) const;
    const Ydb::Value* GetValuePtr(const TString& columnId) const;
    Ydb::Value* GetMutableValuePtr(const TString& columnId);
};

class TTableRecords {
private:
    YDB_READONLY_DEF(std::vector<Ydb::Column>, Columns);
    std::vector<Ydb::Value> Records;

    ui32 AddRecordImpl(const TTableRecord& record);
    Ydb::TypedValue BuildVariableTupleRecords() const;
    Ydb::TypedValue BuildVariableStructRecords() const;
    TString BuildColumnsSchemaTuple() const;
    TString BuildColumnsSchemaStruct() const;

    std::vector<TString> GetColumnIds() const;

    void PopRecord() {
        Y_ABORT_UNLESS(Records.size());
        Records.pop_back();
    }

public:
    TTableRecords SelectColumns(const std::vector<TString>& columnIds) const;
    std::vector<TTableRecord> GetTableRecords() const;

    Ydb::Table::ExecuteDataQueryRequest BuildInsertQuery(const TString& tablePath) const;
    Ydb::Table::ExecuteDataQueryRequest BuildUpsertQuery(const TString& tablePath) const;
    Ydb::Table::ExecuteDataQueryRequest BuildSelectQuery(const TString& tablePath) const;
    Ydb::Table::ExecuteDataQueryRequest BuildDeleteQuery(const TString& tablePath) const;
    Ydb::Table::ExecuteDataQueryRequest BuildUpdateQuery(const TString& tablePath) const;

    void AddColumn(const Ydb::Column& c, const Ydb::Value& v);

    bool empty() const {
        return Records.empty();
    }

    void InitColumns(const std::vector<Ydb::Column>& columns) {
        Y_ABORT_UNLESS(Columns.empty());
        Columns = columns;
    }
    void ReserveRows(const ui32 rowsCount) {
        Records.reserve(rowsCount);
    }
    bool AddRecordAllValues(const TTableRecord& record) {
        if (AddRecordImpl(record) != record.GetValues().size()) {
            PopRecord();
            return false;
        }
        return true;
    }
    bool AddRecordNativeValues(const TTableRecord& record) {
        if (AddRecordImpl(record) != Columns.size()) {
            PopRecord();
            return false;
        }
        return true;
    }
    bool AddRecordSomeValues(const TTableRecord& record) {
        if (!AddRecordImpl(record)) {
            PopRecord();
            return false;
        }
        return true;
    }
};

}

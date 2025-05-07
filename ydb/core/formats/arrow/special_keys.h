#pragma once

#include "arrow_helpers.h"

#include "rows/collection.h"

#include <ydb/library/formats/arrow/replace_key.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow {

class TSpecialKeys {
protected:
    std::shared_ptr<arrow::Schema> Schema;
    TRowsCollection Rows;

    std::shared_ptr<arrow::RecordBatch> BuildBatch() const;

    bool DeserializeFromString(const TString& data);

    TSimpleRow GetKeyByIndex(const ui32 position) const;

    TSpecialKeys() = default;
    TSpecialKeys(const std::shared_ptr<arrow::RecordBatch>& data) {
        Initialize(data);
    }

    void Initialize(const std::shared_ptr<arrow::RecordBatch>& data) {
        Schema = data->schema();
        Rows = TRowsCollection(data);
        AFL_VERIFY(data);
        Y_DEBUG_ABORT_UNLESS(data->ValidateFull().ok());
        AFL_VERIFY(data->num_rows());
        AFL_VERIFY(data->num_columns());
        AFL_VERIFY(data->num_rows() <= 2)("count", data->num_rows());
    }

public:
    TSpecialKeys(const std::vector<TString>& rows, const std::shared_ptr<arrow::Schema>& schema)
        : Schema(schema)
        , Rows(rows) {
    }

    TSimpleRow GetFirst(const bool reverse = false) const {
        if (reverse) {
            return Rows.GetLast(Schema);
        } else {
            return Rows.GetFirst(Schema);
        }
    }

    TSimpleRow GetLast(const bool reverse = false) const {
        if (reverse) {
            return Rows.GetFirst(Schema);
        } else {
            return Rows.GetLast(Schema);
        }
    }

    const std::shared_ptr<arrow::Schema>& GetSchema() const {
        return Schema;
    }

    void Reallocate();

    ui32 GetRecordsCount() const {
        return Rows.GetRecordsCount();
    }

    TSpecialKeys(const TString& data, const std::shared_ptr<arrow::Schema>& schema) {
        auto rbData = NArrow::DeserializeBatch(data, schema);
        Initialize(rbData);
    }

    TSpecialKeys(const TString& data) {
        Y_ABORT_UNLESS(DeserializeFromString(data));
    }

    TString SerializePayloadToString() const;
    TString SerializeFullToString() const;
    TString DebugString() const;
    ui64 GetMemorySize() const;
    ui64 GetDataSize() const;
};

class TFirstLastSpecialKeys: public TSpecialKeys {
private:
    using TBase = TSpecialKeys;

public:
    explicit TFirstLastSpecialKeys(const TString& data);
    explicit TFirstLastSpecialKeys(const TSimpleRow& firstRow, const TSimpleRow& lastRow, const std::shared_ptr<arrow::Schema>& schema)
        : TBase(std::vector<TString>({ firstRow.GetData(), lastRow.GetData() }), schema) {
    }
    explicit TFirstLastSpecialKeys(const TString& firstRow, const TString& lastRow, const std::shared_ptr<arrow::Schema>& schema)
        : TBase(std::vector<TString>({ firstRow, lastRow }), schema) {
    }
    explicit TFirstLastSpecialKeys(const TString& data, const std::shared_ptr<arrow::Schema>& schema)
        : TBase(data, schema) {
    }
    explicit TFirstLastSpecialKeys(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<TString>& columnNames = {});
};

class TMinMaxSpecialKeys: public TSpecialKeys {
private:
    using TBase = TSpecialKeys;

protected:
    TMinMaxSpecialKeys(std::shared_ptr<arrow::RecordBatch> data)
        : TBase(data) {
    }

public:
    TSimpleRow GetMin() const {
        return GetFirst();
    }
    TSimpleRow GetMax() const {
        return GetLast();
    }

    explicit TMinMaxSpecialKeys(const TString& data);
    explicit TMinMaxSpecialKeys(const TString& data, const std::shared_ptr<arrow::Schema>& schema)
        : TBase(data, schema) {
    }

    explicit TMinMaxSpecialKeys(std::shared_ptr<arrow::RecordBatch> batch, const std::shared_ptr<arrow::Schema>& schema);
};

}   // namespace NKikimr::NArrow

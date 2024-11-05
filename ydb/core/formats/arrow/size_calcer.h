#pragma once
#include "special_keys.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/library/formats/arrow/size_calcer.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TBatchSplitttingContext {
private:
    YDB_ACCESSOR(ui64, SizeLimit, 6 * 1024 * 1024);
    YDB_ACCESSOR_DEF(std::vector<TString>, FieldsForSpecialKeys);
public:
    explicit TBatchSplitttingContext(const ui64 size)
        : SizeLimit(size)
    {

    }

    void SetFieldsForSpecialKeys(const std::shared_ptr<arrow::Schema>& schema) {
        std::vector<TString> local;
        for (auto&& i : schema->fields()) {
            local.emplace_back(i->name());
        }
        std::swap(local, FieldsForSpecialKeys);
    }
};

class TSerializedBatch {
private:
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY(ui32, RowsCount, 0);
    YDB_READONLY(ui32, RawBytes, 0);
    std::optional<TString> SpecialKeysFull;
    std::optional<TString> SpecialKeysPayload;

public:
    size_t GetSize() const {
        return Data.size();
    }

    const TString& GetSpecialKeysPayloadSafe() const {
        AFL_VERIFY(SpecialKeysPayload);
        return *SpecialKeysPayload;
    }

    const TString& GetSpecialKeysFullSafe() const {
        AFL_VERIFY(SpecialKeysFull);
        return *SpecialKeysFull;
    }

    bool HasSpecialKeys() const {
        return !!SpecialKeysFull;
    }

    TString DebugString() const;

    static TConclusion<std::vector<TSerializedBatch>> BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context);
    static TConclusionStatus BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context, std::optional<TSerializedBatch>& sbL, std::optional<TSerializedBatch>& sbR);
    static TSerializedBatch Build(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context);

    TSerializedBatch(TString&& data, const ui32 rowsCount, const ui32 rawBytes,
        const std::optional<TString>& specialKeysPayload, const std::optional<TString>& specialKeysFull)
        : Data(data)
        , RowsCount(rowsCount)
        , RawBytes(rawBytes)
        , SpecialKeysFull(specialKeysFull)
        , SpecialKeysPayload(specialKeysPayload) {
        AFL_VERIFY(!!SpecialKeysPayload == !!SpecialKeysFull);
    }
};

TConclusion<std::vector<TSerializedBatch>> SplitByBlobSize(const std::shared_ptr<arrow::RecordBatch>& batch, const TBatchSplitttingContext& context);

}

#pragma once

#include "stats.h"

#include <ydb/core/formats/arrow/container/container.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <ydb/core/formats/arrow/accessor/common/json_value_view.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/json_value_path.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TColumnsData {
private:
    TDictStats Stats;
    YDB_READONLY_DEF(std::shared_ptr<TGeneralContainer>, Records);

public:
    TConclusion<std::shared_ptr<TJsonPathAccessor>> GetPathAccessor(const std::string_view path) const {
        auto jsonPathAccessorTrie = std::make_shared<NKikimr::NArrow::NAccessor::NSubColumns::TJsonPathAccessorTrie>();
        for (ui32 i = 0; i < Stats.GetColumnsCount(); ++i) {
            auto insertResult = jsonPathAccessorTrie->Insert(ToJsonPath(Stats.GetColumnName(i)), Records->GetColumnVerified(i), Stats.GetValueType(i));
            AFL_VERIFY(insertResult.IsSuccess())("error", insertResult.GetErrorMessage());
        }
        return jsonPathAccessorTrie->GetAccessor(path);
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("stats", Stats.DebugJson());
        result.InsertValue("records", Records->DebugJson());
        return result;
    }

    TColumnsData ApplyFilter(const TColumnFilter& filter) const;

    TColumnsData Slice(const ui32 offset, const ui32 count) const;

    static TColumnsData BuildEmpty(const ui32 recordsCount) {
        return TColumnsData(TDictStats::BuildEmpty(), std::make_shared<TGeneralContainer>(recordsCount));
    }

    ui64 GetRawSize() const {
        return Records->GetRawSizeVerified();
    }

    class TIterator {
    private:
        ui32 KeyIndex;
        EValueType ValueType;
        std::shared_ptr<IChunkedArray> GlobalChunkedArray;
        const arrow::Array* CurrentArrayData;
        std::optional<IChunkedArray::TFullChunkedArrayAddress> FullArrayAddress;
        std::optional<IChunkedArray::TFullDataAddress> ChunkAddress;
        ui32 CurrentIndex = 0;

        void InitArrays();

    public:
        TIterator(const ui32 keyIndex, const EValueType valueType, const std::shared_ptr<IChunkedArray>& chunkedArray)
            : KeyIndex(keyIndex)
            , ValueType(valueType)
            , GlobalChunkedArray(chunkedArray) {
            InitArrays();
        }

        ui32 GetCurrentRecordIndex() const {
            return CurrentIndex;
        }

        ui32 GetKeyIndex() const {
            return KeyIndex;
        }

        // Current value is exposed as (array, local index); the reader interprets it per the
        // column's value type.
        const arrow::Array& GetArray() const {
            return *CurrentArrayData;
        }
        i64 GetLocalIndex() const {
            return ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex);
        }

        NArrow::NAccessor::TJsonValueView GetValue() const {
            return ArrayElementToJsonValueView(*CurrentArrayData, GetLocalIndex(), ValueType);
        }

        bool HasValue() const {
            return !CurrentArrayData->IsNull(ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex));
        }

        bool IsValid() const {
            return CurrentIndex < GlobalChunkedArray->GetRecordsCount();
        }

        bool SkipRecordTo(const ui32 recordIndex) {
            if (recordIndex <= CurrentIndex) {
                return true;
            }
            AFL_VERIFY(IsValid());
            AFL_VERIFY(ChunkAddress->GetAddress().Contains(CurrentIndex));
            CurrentIndex = recordIndex;
            for (; CurrentIndex < ChunkAddress->GetAddress().GetGlobalFinishPosition(); ++CurrentIndex) {
                if (CurrentArrayData->IsNull(CurrentIndex - ChunkAddress->GetAddress().GetGlobalStartPosition())) {
                    continue;
                }
                return true;
            }
            InitArrays();
            return IsValid();
        }

        bool Next() {
            AFL_VERIFY(IsValid());
            AFL_VERIFY(ChunkAddress->GetAddress().Contains(CurrentIndex));
            ++CurrentIndex;
            for (; CurrentIndex < ChunkAddress->GetAddress().GetGlobalFinishPosition(); ++CurrentIndex) {
                if (CurrentArrayData->IsNull(CurrentIndex - ChunkAddress->GetAddress().GetGlobalStartPosition())) {
                    continue;
                }
                return true;
            }
            InitArrays();
            return IsValid();
        }
    };

    TIterator BuildIterator(const ui32 keyIndex) const {
        return TIterator(keyIndex, Stats.GetValueType(keyIndex), Records->GetColumnVerified(keyIndex));
    }

    const TDictStats& GetStats() const {
        return Stats;
    }

    TColumnsData(const TDictStats& dict, const std::shared_ptr<TGeneralContainer>& data)
        : Stats(dict)
        , Records(data) {
        AFL_VERIFY(Records->num_columns() == Stats.GetColumnsCount())("records", Records->num_columns())("stats", Stats.GetColumnsCount());
        for (ui32 i = 0; i < (ui32)Records->num_columns(); ++i) {
            AFL_VERIFY(Records->GetColumnVerified(i)->GetDataType()->id() == Stats.GetField(i)->type()->id())(
                "column", Records->GetColumnVerified(i)->GetDataType()->ToString())("stats", Stats.GetField(i)->type()->ToString());
        }
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

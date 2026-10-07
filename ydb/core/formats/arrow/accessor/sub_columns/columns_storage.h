#pragma once

#include "stats.h"

#include <ydb/core/formats/arrow/container/container.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <ydb/core/formats/arrow/accessor/common/json_value_view.h>
#include <ydb/core/formats/arrow/accessor/common/types.h>
#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/json_value_path.h>

#include <util/generic/overloaded.h>

#include <variant>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TColumnsData {
private:
    TDictStats Stats;
    YDB_READONLY_DEF(std::shared_ptr<TGeneralContainer>, Records);

public:
    std::shared_ptr<TJsonPathAccessor> GetPathAccessor(TDictStats::TResolvedPath path) const {
        return std::make_shared<TJsonPathAccessor>(
            Records->GetColumnVerified(path.ColumnIndex), std::move(path.RemainingPath), path.ValueType);
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
        std::variant<const arrow::Array*, const TDictionaryArray*> CurrentData;
        // Currently iterated accessor relative to GlobalChunkedArray
        std::optional<IChunkedArray::TFullChunkedArrayAddress> FullArrayAddress;
        // Currently iterated arrow chunk relative to GlobalChunkedArray
        std::optional<IChunkedArray::TFullDataAddress> ChunkAddress;
        ui32 CurrentIndex = 0;

        void InitArrays();

        bool IsCurrentNull() const {
            return std::visit(TOverloaded{
                [this](const arrow::Array* array) {
                    return array->IsNull(ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex));
                },
                [this](const TDictionaryArray* dictionary) {
                    return dictionary->IsNull(FullArrayAddress->GetAddress().GetLocalIndex(CurrentIndex));
                }},
                CurrentData);
        }

        ui32 GetCurrentFinishPosition() const {
            return std::visit(TOverloaded{
                [this](const arrow::Array*) {
                    return ChunkAddress->GetAddress().GetGlobalFinishPosition();
                },
                [this](const TDictionaryArray*) {
                    return FullArrayAddress->GetAddress().GetGlobalFinishPosition();
                }},
                CurrentData);
        }

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


        NArrow::NAccessor::TJsonValueView GetValue() const {
            return std::visit(TOverloaded{
                [this](const arrow::Array* array) {
                    return ArrayElementToJsonValueView(*array, ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex), ValueType);
                },
                [this](const TDictionaryArray* dictionary) {
                    return dictionary->GetJsonValueView(FullArrayAddress->GetAddress().GetLocalIndex(CurrentIndex), ValueType);
                }},
                CurrentData);
        }


        bool HasValue() const {
            return !IsCurrentNull();
        }

        bool IsValid() const {
            return CurrentIndex < GlobalChunkedArray->GetRecordsCount();
        }

        bool SkipRecordTo(const ui32 recordIndex) {
            if (recordIndex <= CurrentIndex) {
                return true;
            }
            AFL_VERIFY(IsValid());
            CurrentIndex = recordIndex;
            for (; CurrentIndex < GetCurrentFinishPosition(); ++CurrentIndex) {
                if (IsCurrentNull()) {
                    continue;
                }
                return true;
            }
            InitArrays();
            return IsValid();
        }

        bool Next() {
            AFL_VERIFY(IsValid());
            ++CurrentIndex;
            for (; CurrentIndex < GetCurrentFinishPosition(); ++CurrentIndex) {
                if (IsCurrentNull()) {
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

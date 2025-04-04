#pragma once

#include "stats.h"

#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TColumnsData {
private:
    TDictStats Stats;
    YDB_READONLY_DEF(std::shared_ptr<TGeneralContainer>, Records);

public:
    std::shared_ptr<IChunkedArray> GetPathAccessor(const std::string_view path) const {
        auto idx = Stats.GetKeyIndexOptional(path);
        if (!idx) {
            return nullptr;
        } else {
            return Records->GetColumnVerified(*idx);
        }
    }

    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("stats", Stats.DebugJson());
        result.InsertValue("records", Records->DebugJson(true));
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
        std::shared_ptr<IChunkedArray> GlobalChunkedArray;
        const arrow::StringArray* CurrentArrayData;
        std::optional<IChunkedArray::TFullChunkedArrayAddress> FullArrayAddress;
        std::optional<IChunkedArray::TFullDataAddress> ChunkAddress;
        ui32 CurrentIndex = 0;

        void InitArrays() {
            while (CurrentIndex < GlobalChunkedArray->GetRecordsCount()) {
                if (!FullArrayAddress || !FullArrayAddress->GetAddress().Contains(CurrentIndex)) {
                    FullArrayAddress = GlobalChunkedArray->GetArray(FullArrayAddress, CurrentIndex, GlobalChunkedArray);
                    ChunkAddress = std::nullopt;
                }
                const ui32 localIndex = FullArrayAddress->GetAddress().GetLocalIndex(CurrentIndex);
                ChunkAddress = FullArrayAddress->GetArray()->GetChunk(ChunkAddress, localIndex);
                AFL_VERIFY(ChunkAddress->GetArray()->type()->id() == arrow::utf8()->id());
                CurrentArrayData = static_cast<const arrow::StringArray*>(ChunkAddress->GetArray().get());
                if (FullArrayAddress->GetArray()->GetType() == IChunkedArray::EType::Array) {
                    if (CurrentArrayData->IsNull(localIndex)) {
                        Next();
                    }
                    break;
                } else if (FullArrayAddress->GetArray()->GetType() == IChunkedArray::EType::SparsedArray) {
                    if (CurrentArrayData->IsNull(localIndex) &&
                        std::static_pointer_cast<TSparsedArray>(FullArrayAddress->GetArray())->GetDefaultValue() == nullptr) {
                        CurrentIndex += ChunkAddress->GetArray()->length();
                    } else {
                        break;
                    }
                } else {
                    AFL_VERIFY(false)("type", FullArrayAddress->GetArray()->GetType());
                }
            }
            AFL_VERIFY(CurrentIndex <= GlobalChunkedArray->GetRecordsCount());
        }

    public:
        TIterator(const ui32 keyIndex, const std::shared_ptr<IChunkedArray>& chunkedArray)
            : KeyIndex(keyIndex)
            , GlobalChunkedArray(chunkedArray) {
            InitArrays();
        }

        ui32 GetCurrentRecordIndex() const {
            return CurrentIndex;
        }

        ui32 GetKeyIndex() const {
            return KeyIndex;
        }

        std::string_view GetValue() const {
            auto view = CurrentArrayData->GetView(ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex));
            return std::string_view(view.data(), view.size());
        }

        bool HasValue() const {
            return !CurrentArrayData->IsNull(ChunkAddress->GetAddress().GetLocalIndex(CurrentIndex));
        }

        bool IsValid() const {
            return CurrentIndex < GlobalChunkedArray->GetRecordsCount();
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
        return TIterator(keyIndex, Records->GetColumnVerified(keyIndex));
    }

    const TDictStats& GetStats() const {
        return Stats;
    }

    TColumnsData(const TDictStats& dict, const std::shared_ptr<TGeneralContainer>& data)
        : Stats(dict)
        , Records(data) {
        AFL_VERIFY(Records->num_columns() == Stats.GetColumnsCount())("records", Records->num_columns())("stats", Stats.GetColumnsCount());
        for (auto&& i : Records->GetColumns()) {
            AFL_VERIFY(i->GetDataType()->id() == arrow::utf8()->id());
        }
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

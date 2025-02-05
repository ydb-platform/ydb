#pragma once

#include "stats.h"

#include <ydb/core/formats/arrow/common/container.h>

#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

class TColumnsData {
private:
    TDictStats Stats;
    YDB_READONLY_DEF(std::shared_ptr<TGeneralContainer>, Records);

public:
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("stats", Stats.DebugJson());
        result.InsertValue("records", Records->DebugJson(true));
        return result;
    }

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
        std::shared_ptr<IChunkedArray> ChunkedArray;
        std::shared_ptr<arrow::StringArray> CurrentArray;
        IChunkedArray::TReader Reader;
        IChunkedArray::TAddress CurrentAddress;
        ui32 CurrentIndex = 0;

        void InitArrays() {
            while (CurrentIndex < ChunkedArray->GetRecordsCount()) {
                CurrentAddress = Reader.GetReadChunk(CurrentIndex);
                AFL_VERIFY(CurrentAddress.GetPosition() == 0);
                AFL_VERIFY(CurrentAddress.GetArray()->type()->id() == arrow::utf8()->id());
                CurrentArray = std::static_pointer_cast<arrow::StringArray>(CurrentAddress.GetArray());
                if (ChunkedArray->GetType() == IChunkedArray::EType::Array) {
                    if (CurrentArray->IsNull(0)) {
                        Next();
                    }
                    break;
                } else if (ChunkedArray->GetType() == IChunkedArray::EType::SparsedArray) {
                    if (CurrentArray->IsNull(0)) {
                        CurrentIndex += CurrentAddress.GetArray()->length();
                    } else {
                        break;
                    }
                } else {
                    AFL_VERIFY(false)("type", ChunkedArray->GetType());
                }
            }
        }

    public:
        TIterator(const ui32 keyIndex, const std::shared_ptr<IChunkedArray>& chunkedArray)
            : KeyIndex(keyIndex)
            , ChunkedArray(chunkedArray)
            , Reader(ChunkedArray)
            , CurrentAddress(Reader.GetReadChunk(0)) {
            InitArrays();
        }

        ui32 GetCurrentRecordIndex() const {
            return CurrentIndex;
        }

        ui32 GetKeyIndex() const {
            return KeyIndex;
        }

        std::string_view GetValue() const {
            auto view = CurrentArray->GetView(CurrentAddress.GetPosition());
            return std::string_view(view.data(), view.size());
        }

        bool HasValue() const {
            return !CurrentArray->IsNull(CurrentAddress.GetPosition());
        }

        bool IsValid() const {
            return CurrentIndex < ChunkedArray->GetRecordsCount();
        }

        bool Next() {
            AFL_VERIFY(IsValid());
            while (true) {
                if (CurrentAddress.NextPosition()) {
                    AFL_VERIFY(++CurrentIndex < ChunkedArray->GetRecordsCount());
                    if (CurrentArray->IsNull(CurrentIndex)) {
                        continue;
                    }
                    return true;
                } else if (++CurrentIndex == ChunkedArray->GetRecordsCount()) {
                    return false;
                } else {
                    InitArrays();
                    return IsValid();
                }
            }
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
        AFL_VERIFY(Records->num_columns() == Stats.GetColumnsCount());
        for (auto&& i : Records->GetColumns()) {
            AFL_VERIFY(i->GetDataType()->id() == arrow::utf8()->id());
        }
    }
};

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

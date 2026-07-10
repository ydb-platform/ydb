#include "accessor.h"
#include "columns_storage.h"
#include "direct_builder.h"

#include <util/string/escape.h>
#include <ydb/core/formats/arrow/accessor/common/chunk_data.h>
#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>

#include <contrib/libs/simdjson/include/simdjson.h>

namespace NKikimr::NArrow::NAccessor::NSubColumns {

void TColumnElements::BuildPlainAccessor(const ui32 recordsCount, const EValueType valueType) {
    AFL_VERIFY(!Accessor);
    const auto codec = GetCodecForValueType(valueType);
    auto builder = codec->MakeBuilder(recordsCount, DataSize);
    ui32 nextExpected = 0;
    for (ui32 i = 0; i < RecordIndexes.size(); ++i) {
        AFL_VERIFY(nextExpected <= RecordIndexes[i]);
        TStatusValidator::Validate(builder->AppendNulls(RecordIndexes[i] - nextExpected));
        codec->AppendFromBinaryJson(*builder, Values[i]);
        nextExpected = RecordIndexes[i] + 1;
    }
    AFL_VERIFY(nextExpected <= recordsCount);
    TStatusValidator::Validate(builder->AppendNulls(recordsCount - nextExpected));
    Accessor = std::make_shared<TTrivialArray>(NArrow::FinishBuilder(std::move(builder)));
}

void TColumnElements::BuildDictionaryAccessor(const ui32 recordsCount, const EValueType valueType) {
    BuildPlainAccessor(recordsCount, valueType);
    const TChunkConstructionData cData(recordsCount, nullptr, GetCodecForValueType(valueType)->GetArrowType(),
        NSerialization::TSerializerContainer::GetDefaultSerializer());
    Accessor = NDictionary::TConstructor().Construct(Accessor, cData).DetachResult();
}

std::shared_ptr<TSubColumnsArray> TDataBuilder::Finish() {
    std::map<ui64, std::vector<TColumnElements*>> elementsBySize;
    ui64 sumSize = 0;
    for (auto&& i : Elements) {
        elementsBySize[i.second.GetDataSize()].emplace_back(&i.second);
        sumSize += i.second.GetDataSize();
    }
    std::vector<TColumnElements*> columnElements;
    std::vector<TColumnElements*> otherElements;
    TSettings::TColumnsDistributor distributor = Settings.BuildDistributor(sumSize, CurrentRecordIndex);
    for (auto rIt = elementsBySize.rbegin(); rIt != elementsBySize.rend(); ++rIt) {
        for (auto&& i : rIt->second) {
            switch (distributor.TakeAndDetect(rIt->first, i->GetRecordIndexes().size())) {
                case TSettings::TColumnsDistributor::EColumnType::Separated:
                    columnElements.emplace_back(i);
                    break;
                case TSettings::TColumnsDistributor::EColumnType::Other:
                    otherElements.emplace_back(i);
                    break;
            }
        }
    }
    const auto predSortElements = [](const TColumnElements* l, const TColumnElements* r) {
        return l->GetKeyName() < r->GetKeyName();
    };
    std::sort(columnElements.begin(), columnElements.end(), predSortElements);
    std::sort(otherElements.begin(), otherElements.end(), predSortElements);
    TDictStats columnStats = BuildStats(columnElements, Settings, true);
    {
        ui32 columnIdx = 0;
        for (auto&& i : columnElements) {
            const EValueType valueType = columnStats.GetValueType(columnIdx);
            switch (columnStats.GetAccessorType(columnIdx)) {
                case IChunkedArray::EType::Array:
                    i->BuildPlainAccessor(CurrentRecordIndex, valueType);
                    break;
                case IChunkedArray::EType::Dictionary:
                    i->BuildDictionaryAccessor(CurrentRecordIndex, valueType);
                    break;
                // Sparsed is disabled for subcolumns
                case IChunkedArray::EType::SparsedArray:
                case IChunkedArray::EType::Undefined:
                case IChunkedArray::EType::SerializedChunkedArray:
                case IChunkedArray::EType::CompositeChunkedArray:
                case IChunkedArray::EType::SubColumnsArray:
                case IChunkedArray::EType::SubColumnsPartialArray:
                case IChunkedArray::EType::ChunkedArray:
                    AFL_VERIFY(false);
            }
            ++columnIdx;
        }
    }

    TOthersData rbOthers = MergeOthers(otherElements);

    auto records = std::make_shared<TGeneralContainer>(CurrentRecordIndex);
    for (size_t idx = 0; idx < columnElements.size(); ++idx) {
        records->AddField(columnStats.GetField(idx), columnElements[idx]->GetAccessorVerified()).Validate();
    }
    TColumnsData cData(std::move(columnStats), std::move(records));
    return std::make_shared<TSubColumnsArray>(std::move(cData), std::move(rbOthers), Type, CurrentRecordIndex, Settings);
}

TOthersData TDataBuilder::MergeOthers(const std::vector<TColumnElements*>& otherKeys) const {
    std::vector<THeapElements> heap;
    ui32 idx = 0;
    for (auto&& i : otherKeys) {
        heap.emplace_back(i, idx);
        AFL_VERIFY(heap.back().IsValid());
        ++idx;
    }
    std::make_heap(heap.begin(), heap.end());
    auto othersBuilder = TOthersData::MakeMergedBuilder();
    while (heap.size()) {
        std::pop_heap(heap.begin(), heap.end());
        std::string_view view = heap.back().GetValuePointer() ?
            std::string_view(heap.back().GetValuePointer()->Data(), heap.back().GetValuePointer()->Size()) : "";
        std::string_view* viewPtr = heap.back().GetValuePointer() ? &view : nullptr;
        othersBuilder->AddImpl(heap.back().GetRecordIndex(), heap.back().GetKeyIndex(), viewPtr);
        if (!heap.back().Next()) {
            heap.pop_back();
        } else {
            std::push_heap(heap.begin(), heap.end());
        }
    }
    return othersBuilder->Finish(TOthersData::TFinishContext(BuildStats(otherKeys, Settings, false)));
}

std::string BuildString(const TStringBuf currentPrefix, const TStringBuf key) {
    TStringBuilder builder;
    const auto escapedKey = QuoteJsonItem(key);
    if (currentPrefix.size()) {
        builder << currentPrefix << ".";
    }
    builder << escapedKey;

    return builder;
}

TStringBuf TDataBuilder::AddKeyOwn(const TStringBuf currentPrefix, std::string&& key) {
    auto it = StorageHash.find(TStorageAddress(currentPrefix, TStringBuf(key.data(), key.size())));
    if (it == StorageHash.end()) {
        Storage.emplace_back(std::move(key));
        TStringBuf sbKey(Storage.back().data(), Storage.back().size());
        it = StorageHash.emplace(TStorageAddress(currentPrefix, sbKey), BuildString(currentPrefix, sbKey)).first;
    }
    return TStringBuf(it->second.data(), it->second.size());
}

TStringBuf TDataBuilder::AddKey(const TStringBuf currentPrefix, const TStringBuf key) {
    TStorageAddress keyAddress(currentPrefix, key);
    auto it = StorageHash.find(keyAddress);
    if (it == StorageHash.end()) {
        it = StorageHash.emplace(keyAddress, BuildString(currentPrefix, key)).first;
    }
    return TStringBuf(it->second.data(), it->second.size());
}

TDataBuilder::TDataBuilder(const std::shared_ptr<arrow::DataType>& type, const TSettings& settings)
    : Type(type)
    , Settings(settings) {
}

}   // namespace NKikimr::NArrow::NAccessor::NSubColumns

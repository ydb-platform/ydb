#include "logic.h"

#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction::NDictionary {

void TMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& /*mergingContext*/) {
    for (auto&& i : input) {
        std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> arrList;
        NArrow::NAccessor::IChunkedArray::VisitDataOwners<bool>(i, [&arrList](const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& arr) {
            arrList.emplace_back(arr);
            return false;
        });
        Iterators.emplace_back(
            TIterator(std::make_shared<NArrow::NAccessor::TCompositeChunkedArray>(std::move(arrList), i->GetRecordsCount(), i->GetDataType()),
                Context.GetLoader()));
    }
    RemapIndexes.resize(input.size());
    AFL_VERIFY(NArrow::SwitchType(Context.GetResultField()->type()->id(), [&](const auto type) {
        if constexpr (type.IsAppropriate) {
            std::map<typename decltype(type)::ValueType, ui32> globalDecoder;
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> chunks;
            for (ui32 idx = 0; idx < input.size(); ++idx) {
                if (Iterators[idx].IsValid()) {
                    while (true) {
                        AFL_VERIFY(Iterators[idx].GetCurrentDataChunk().GetType() == NArrow::NAccessor::IChunkedArray::EType::Dictionary);
                        const auto* dict = static_cast<const NArrow::NAccessor::TDictionaryArray*>(&Iterators[idx].GetCurrentDataChunk());
                        const auto* arrVariants = type.CastArray(dict->GetVariants().get());
                        for (ui32 r = 0; r < (ui32)arrVariants->length(); ++r) {
                            auto it = globalDecoder.emplace(type.GetValue(*arrVariants, r), globalDecoder.size()).first;
                            RemapIndexes[idx].emplace_back(it->second);
                        }
                        if (!Iterators[idx].MoveFurther(Iterators[idx].GetCurrentDataChunk().GetRecordsCount())) {
                            break;
                        }
                    }
                }
            }
            std::vector<i32> remap;
            remap.resize(globalDecoder.size(), -1);
            {
                ui32 decodeIdx = 0;
                for (auto&& i : globalDecoder) {
                    remap[i.second] = decodeIdx++;
                }
            }
            for (auto&& input : RemapIndexes) {
                for (auto&& v : input) {
                    AFL_VERIFY(remap[v] >= 0);
                    AFL_VERIFY((ui32)remap[v] < globalDecoder.size());
                    v = remap[v];
                }
            }
            auto builder = NArrow::MakeBuilder(Context.GetResultField()->type());
            auto* builderImpl = type.CastBuilder(builder.get());
            for (auto&& i : globalDecoder) {
                NArrow::TStatusValidator::Validate(builderImpl->Append(i.first));
            }
            ArrayVariantsFull = NArrow::FinishBuilder(std::move(builder));
            return true;
        }
        return false;
    }));
    for (auto&& i : Iterators) {
        i.Reset();
    }
}

TColumnPortionResult TMerger::DoExecute(const TChunkMergeContext& chunkContext, TMergingContext& /*mergeContext*/) {
    std::vector<bool> mask(ArrayVariantsFull->length(), false);
    ui32 maskSize = 0;
    std::vector<i32> records;
    records.reserve(chunkContext.GetRemapper().GetRecordsCount());
    for (ui32 resultRecordIdx = 0; resultRecordIdx < chunkContext.GetRemapper().GetRecordsCount(); ++resultRecordIdx) {
        const ui32 inputIdx = chunkContext.GetRemapper().GetIdxArray().Value(resultRecordIdx);
        const ui32 inputRecordIdx = chunkContext.GetRemapper().GetRecordIdxArray().Value(resultRecordIdx);
        if (!Iterators[inputIdx].IsEmpty()) {
            AFL_VERIFY(Iterators[inputIdx].MoveToPosition(inputRecordIdx));
            AFL_VERIFY(NArrow::SwitchType(Iterators[inputIdx].GetCurrentRecordsType(), [&](const auto type) {
                const auto* arr = type.CastArray(Iterators[inputIdx].GetCurrentDataChunk().GetRecords().get());
                if constexpr (type.IsIndexType()) {
                    if (arr->IsNull(Iterators[inputIdx].GetLocalPosition())) {
                        records.emplace_back(-1);
                    } else {
                        const ui32 dictIdx = type.GetValue(*arr, Iterators[inputIdx].GetLocalPosition());
                        AFL_VERIFY(inputIdx < RemapIndexes.size());
                        AFL_VERIFY(dictIdx < RemapIndexes[inputIdx].size())("size", RemapIndexes[inputIdx].size())("idx", dictIdx);
                        const ui32 finalValueIndex = RemapIndexes[inputIdx][dictIdx];
                        records.emplace_back(finalValueIndex);
                        AFL_VERIFY(finalValueIndex < mask.size());
                        if (!mask[finalValueIndex]) {
                            mask[finalValueIndex] = true;
                            ++maskSize;
                        }
                    }
                    return true;
                }
                return false;
            }))("type", Iterators[inputIdx].GetCurrentDataChunk().GetDataType()->ToString());
        } else {
            records.emplace_back(-1);
        }
    }
    std::shared_ptr<NArrow::NAccessor::TDictionaryArray> dictArr;
    auto rBuilder = NArrow::MakeBuilder(NArrow::NAccessor::NDictionary::TConstructor::GetTypeByVariantsCount(maskSize));
    if (maskSize == ArrayVariantsFull->length()) {
        AFL_VERIFY(NArrow::SwitchType(rBuilder->type()->id(), [&](const auto type) {
            if constexpr (type.IsIndexType()) {
                auto* builderImpl = type.CastBuilder(rBuilder.get());
                for (auto&& r : records) {
                    if (r >= 0) {
                        NArrow::TStatusValidator::Validate(builderImpl->Append(r));
                    } else {
                        NArrow::TStatusValidator::Validate(builderImpl->AppendNull());
                    }
                }
                return true;
            }
            return false;
            }));
        dictArr = std::make_shared<NArrow::NAccessor::TDictionaryArray>(ArrayVariantsFull, NArrow::FinishBuilder(std::move(rBuilder)));
    } else {
        std::vector<i32> remap;
        remap.resize(mask.size(), -1);
        ui32 approveIdx = 0;
        for (ui32 i = 0; i < mask.size(); ++i) {
            if (mask[i]) {
                remap[i] = approveIdx++;
            }
        }
        auto rBuilder = NArrow::MakeBuilder(NArrow::NAccessor::NDictionary::TConstructor::GetTypeByVariantsCount(maskSize));
        AFL_VERIFY(NArrow::SwitchType(rBuilder->type()->id(), [&](const auto type) {
            if constexpr (type.IsIndexType()) {
                auto* builderImpl = type.CastBuilder(rBuilder.get());
                for (auto&& r : records) {
                    if (r < 0) {
                        NArrow::TStatusValidator::Validate(builderImpl->AppendNull());
                    } else {
                        AFL_VERIFY((ui32)r < remap.size());
                        AFL_VERIFY(remap[r] >= 0);
                        NArrow::TStatusValidator::Validate(builderImpl->Append(remap[r]));
                    }
                }
                return true;
            }
            return false;
            }));

        auto arr = NArrow::TColumnFilter(std::move(mask))
                       .Apply(std::make_shared<NArrow::NAccessor::TTrivialArray>(ArrayVariantsFull))
                       ->GetChunkedArray();
        AFL_VERIFY(arr && arr->num_chunks() == 1);
        dictArr = std::make_shared<NArrow::NAccessor::TDictionaryArray>(arr->chunk(0), NArrow::FinishBuilder(std::move(rBuilder)));
    }
    IColumnMerger::TPortionColumnChunkWriter<NArrow::NAccessor::TDictionaryArray, NArrow::NAccessor::NDictionary::TConstructor> col(
        NArrow::NAccessor::NDictionary::TConstructor(), Context.GetColumnId());
    col.AddChunk(dictArr, Context);
    return col;
}

}   // namespace NKikimr::NOlap::NCompaction::NDictionary

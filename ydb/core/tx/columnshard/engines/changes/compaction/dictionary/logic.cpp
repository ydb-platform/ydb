#include "logic.h"

#include <ydb/core/formats/arrow/accessor/dictionary/constructor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/arrow_filter.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction::NDictionary {

void TMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& /*mergingContext*/) {
    for (auto&& i : input) {
        Iterators.emplace_back(TIterator(i));
    }
    RemapIndexes.resize(input.size());
    AFL_VERIFY(NArrow::SwitchType(Context.GetResultField()->type()->id(), [&](const auto type) {
        if constexpr(type.IsAppropriate) {
            std::map<typename decltype(type)::ValueType, ui32> globalDecoder;
            std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> chunks;
            for (ui32 idx = 0; idx < input.size(); ++idx) {
                if (!input[idx]) {
                    continue;
                }
                while (Iterators[idx].IsValid()) {
                    AFL_VERIFY(Iterators[idx].GetCurrentDataChunk().GetType() == NArrow::NAccessor::IChunkedArray::EType::Dictionary);
                    const auto* dict = static_cast<const NArrow::NAccessor::TDictionaryArray*>(&Iterators[idx].GetCurrentDataChunk());
                    const auto* arrVariants = type.CastArray(dict->GetVariants().get());
                    for (ui32 r = 0; r < (ui32)arrVariants->length(); ++r) {
                        auto it = globalDecoder.emplace(type.GetValue(*arrVariants, r), globalDecoder.size()).first;
                        RemapIndexes[idx].emplace_back(it->second);
                    }
                    Iterators[idx].MoveFurther(Iterators[idx].GetCurrentDataChunk().GetRecordsCount());
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

std::vector<TColumnPortionResult> TMerger::DoExecute(const TChunkMergeContext& /*chunkContext*/, TMergingContext& mergeContext) {
    std::vector<TColumnPortionResult> result;
    for (auto&& i : mergeContext.GetChunks()) {
        std::vector<bool> mask(ArrayVariantsFull->length(), false);
        ui32 maskSize = 0;
        std::vector<ui32> records;
        for (ui32 idx = 0; idx < i.GetIdxArray().length(); ++idx) {
            const ui32 inputIdx = i.GetIdxArray().Value(idx);
            const ui32 inputRecordIdx = i.GetRecordIdxArray().Value(idx);
            Iterators[inputIdx].MoveToPosition(inputRecordIdx);
            AFL_VERIFY(NArrow::SwitchType(Iterators[idx].GetCurrentDataChunk().GetDataType()->id(), [&](const auto type) {
                const auto* arr = type.CastArray(Iterators[idx].GetCurrentDataChunk().GetRecords().get());
                if constexpr (type.IsIndexType()) {
                    const auto dictIdx = type.GetValue(*arr, Iterators[idx].GetChunkPosition());
                    records.emplace_back(dictIdx);
                    AFL_VERIFY(inputIdx < RemapIndexes.size());
                    AFL_VERIFY(dictIdx < RemapIndexes[inputIdx].size());
                    if (!mask[RemapIndexes[inputIdx][dictIdx]]) {
                        mask[RemapIndexes[inputIdx][dictIdx]] = true;
                        ++maskSize;
                    }
                    return true;
                }
                return false;
            }));
        }

        std::shared_ptr<NArrow::NAccessor::TDictionaryArray> dictArr;
        auto rBuilder = NArrow::MakeBuilder(NArrow::NAccessor::NDictionary::TConstructor::GetTypeByVariantsCount(maskSize));
        if (maskSize == ArrayVariantsFull->length()) {
            AFL_VERIFY(NArrow::SwitchType(rBuilder->type()->id(), [&](const auto type) {
                if constexpr (type.IsIndexType()) {
                    auto* builderImpl = type.CastBuilder(rBuilder.get());
                    for (auto&& r : records) {
                        NArrow::TStatusValidator::Validate(builderImpl->Append(r));
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
                        AFL_VERIFY(r < remap.size());
                        AFL_VERIFY(remap[r] >= 0);
                        NArrow::TStatusValidator::Validate(builderImpl->Append(remap[r]));
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
        TPortionColumn<NArrow::NAccessor::TDictionaryArray, NArrow::NAccessor::NDictionary::TConstructor> col(
            NArrow::NAccessor::NDictionary::TConstructor(), Context.GetColumnId());
        col.AddChunk(dictArr, Context);
        result.emplace_back(col);
    }
    return result;
}

}   // namespace NKikimr::NOlap::NCompaction::NDictionary

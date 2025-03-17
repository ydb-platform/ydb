#include "bits_storage.h"
#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes {

TString TBloomIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 /*recordsCount*/) const {
    std::deque<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> dataOwners;
    ui32 indexHitsCount = 0;
    for (reader.Start(); reader.IsCorrect();) {
        AFL_VERIFY(reader.GetColumnsCount() == 1);
        for (auto&& i : reader) {
            dataOwners.emplace_back(i.GetCurrentChunk());
            auto indexHitsCountLocal = GetDataExtractor()->GetIndexHitsCount(dataOwners.back());
            for (auto&& hc : indexHitsCountLocal) {
                indexHitsCount += hc.second;
            }
        }
        reader.ReadNext(reader.begin()->GetCurrentChunk()->GetRecordsCount());
    }
    const ui32 bitsCount = TFixStringBitsStorage::GrowBitsCountToByte(HashesCount * std::max<ui32>(indexHitsCount, 10) / std::log(2));
    std::vector<bool> filterBits(bitsCount, false);

    const auto predNoBase = [&](const ui64 hash, const ui32 /*idx*/) {
        filterBits[hash % bitsCount] = true;
    };
    while (dataOwners.size()) {
        GetDataExtractor()->VisitAll(
            dataOwners.front(),
            [&](const std::shared_ptr<arrow::Array>& arr, const ui64 hashBase) {
                for (ui64 i = 0; i < HashesCount; ++i) {
                    if (hashBase) {
                        const auto predWithBase = [&](const ui64 hash, const ui32 /*idx*/) {
                            filterBits[CombineHashes(hashBase, hash) % bitsCount] = true;
                        };
                        NArrow::NHash::TXX64::CalcForAll(arr, i, predWithBase);
                    } else {
                        NArrow::NHash::TXX64::CalcForAll(arr, i, predNoBase);
                    }
                }
            },
            [&](const std::string_view data, const ui64 hashBase) {
                for (ui64 i = 0; i < HashesCount; ++i) {
                    const ui64 hash = NArrow::NHash::TXX64::CalcSimple(data, i);
                    if (hashBase) {
                        filterBits[CombineHashes(hashBase, hash) % bitsCount] = true;
                    } else {
                        filterBits[hash % bitsCount] = true;
                    }
                }
            });
        dataOwners.pop_front();
    }

    return TFixStringBitsStorage(filterBits).GetData();
}

bool TBloomIndexMeta::DoCheckValue(
    const TString& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const {
    std::set<ui64> hashes;
    AFL_VERIFY(op == EOperation::Equals)("op", op);
    TFixStringBitsStorage bits(data);
    if (!!category) {
        for (ui64 hashSeed = 0; hashSeed < HashesCount; ++hashSeed) {
            const ui64 hash = NArrow::NHash::TXX64::CalcForScalar(value, hashSeed);
            if (!bits.Get(CombineHashes(*category, hash) % bits.GetSizeBits())) {
                return false;
            }
        }
    } else {
        for (ui64 hashSeed = 0; hashSeed < HashesCount; ++hashSeed) {
            const ui64 hash = NArrow::NHash::TXX64::CalcForScalar(value, hashSeed);
            if (!bits.Get(hash % bits.GetSizeBits())) {
                return false;
            }
        }
    }
    return true;
}

std::optional<ui64> TBloomIndexMeta::DoCalcCategory(const TString& subColumnName) const {
    ui64 result;
    const NRequest::TOriginalDataAddress addr(Max<ui32>(), subColumnName);
    AFL_VERIFY(GetDataExtractor()->CheckForIndex(addr, result));
    if (subColumnName) {
        return result;
    } else {
        return std::nullopt;
    }
}

}   // namespace NKikimr::NOlap::NIndexes

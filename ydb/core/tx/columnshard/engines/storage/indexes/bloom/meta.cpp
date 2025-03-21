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
    TDynBitMap filterBits;
    filterBits.Reserve(HashesCount * std::max<ui32>(indexHitsCount, 10) / std::log(2));
    const ui64 bitsCount = filterBits.Size();

    const auto predNoBase = [&](const ui64 hash, const ui32 /*idx*/) {
        filterBits.Set(hash % bitsCount);
    };
    while (dataOwners.size()) {
        GetDataExtractor()->VisitAll(
            dataOwners.front(),
            [&](const std::shared_ptr<arrow::Array>& arr, const ui64 hashBase) {
                for (ui64 i = 0; i < HashesCount; ++i) {
                    if (hashBase) {
                        const auto predWithBase = [&](const ui64 hash, const ui32 /*idx*/) {
                            filterBits.Set(CombineHashes(hashBase, hash) % bitsCount);
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

    return GetBitsStorageConstructor()->Build(std::move(filterBits))->SerializeToString();
}

bool TBloomIndexMeta::DoCheckValueImpl(
    const IBitsStorage& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value, const EOperation op) const {
    std::set<ui64> hashes;
    AFL_VERIFY(op == EOperation::Equals)("op", op);
    const ui32 bitsCount = data.GetBitsCount();
    if (!!category) {
        for (ui64 hashSeed = 0; hashSeed < HashesCount; ++hashSeed) {
            const ui64 hash = NArrow::NHash::TXX64::CalcForScalar(value, hashSeed);
            if (!data.Get(CombineHashes(*category, hash) % bitsCount)) {
                return false;
            }
        }
    } else {
        for (ui64 hashSeed = 0; hashSeed < HashesCount; ++hashSeed) {
            const ui64 hash = NArrow::NHash::TXX64::CalcForScalar(value, hashSeed);
            if (!data.Get(hash % bitsCount)) {
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

TConclusionStatus TBloomIndexMeta::DoCheckModificationCompatibility(const IIndexMeta& newMeta) const {
    const auto* bMeta = dynamic_cast<const TBloomIndexMeta*>(&newMeta);
    if (!bMeta) {
        return TConclusionStatus::Fail(
            "cannot read meta as appropriate class: " + GetClassName() + ". Meta said that class name is " + newMeta.GetClassName());
    }
    AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
    return TBase::CheckSameColumnsForModification(newMeta);
}

bool TBloomIndexMeta::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
    AFL_VERIFY(TBase::DoDeserializeFromProto(proto));
    AFL_VERIFY(proto.HasBloomFilter());
    auto& bFilter = proto.GetBloomFilter();
    {
        auto conclusion = TBase::DeserializeFromProtoImpl(bFilter);
        if (conclusion.IsFail()) {
            AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("index_parsing", conclusion.GetErrorMessage());
            return false;
        }
    }
    FalsePositiveProbability = bFilter.GetFalsePositiveProbability();
    for (auto&& i : bFilter.GetColumnIds()) {
        AddColumnId(i);
    }
    if (!MutableDataExtractor().DeserializeFromProto(bFilter.GetDataExtractor())) {
        return false;
    }
    Initialize();
    return true;
}

void TBloomIndexMeta::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const {
    auto* filterProto = proto.MutableBloomFilter();
    TBase::SerializeToProtoImpl(*filterProto);
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    for (auto&& i : GetColumnIds()) {
        filterProto->AddColumnIds(i);
    }
    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

void TBloomIndexMeta::Initialize() {
    AFL_VERIFY(!ResultSchema);
    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>(
        "", arrow::TypeTraits<arrow::BooleanType>::type_singleton()) };
    ResultSchema = std::make_shared<arrow::Schema>(fields);
    AFL_VERIFY(FalsePositiveProbability < 1 && FalsePositiveProbability >= 0.01);
    HashesCount = -1 * std::log(FalsePositiveProbability) / std::log(2);
}

}   // namespace NKikimr::NOlap::NIndexes

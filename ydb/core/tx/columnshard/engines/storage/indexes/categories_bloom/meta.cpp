#include "header.h"
#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/columnshard/engines/protos/index.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/data.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

class TCategoryBuilder {
private:
    YDB_READONLY_DEF(std::set<ui64>, Categories);
    YDB_ACCESSOR_DEF(TDynBitMap, Filter);

public:
    TCategoryBuilder(std::set<ui64>&& categories, const ui32 count, const ui32 hashesCount)
        : Categories(categories) {
        AFL_VERIFY(count);
        const ui32 bitsCount = hashesCount * std::max<ui32>(count, 10) / std::log(2);
        AFL_VERIFY(bitsCount);
        Filter.Reserve(bitsCount);
    }
};

class TFiltersBuilder {
private:
    YDB_ACCESSOR_DEF(std::deque<TCategoryBuilder>, Builders);
    THashMap<ui64, TDynBitMap*> FiltersByHash;

public:
    TDynBitMap& MutableFilter(const ui64 hashBase) {
        auto it = FiltersByHash.find(hashBase);
        AFL_VERIFY(it != FiltersByHash.end());
        return *it->second;
    }

    void AddCategory(std::set<ui64>&& categories, const ui32 count, const ui32 hashesCount) {
        Builders.emplace_back(std::move(categories), count, hashesCount);
        for (auto&& i : Builders.back().GetCategories()) {
            AFL_VERIFY(FiltersByHash.emplace(i, &Builders.back().MutableFilter()).second);
        }
    }
};

class TCategoriesBuilder {
private:
    THashMap<ui64, ui32> CountByHash;

public:
    void AddHash(const ui64 hashBase, const ui32 hitsCount) {
        auto it = CountByHash.find(hashBase);
        if (it == CountByHash.end()) {
            it = CountByHash.emplace(hashBase, 0).first;
        }
        it->second += hitsCount;
    }

    [[nodiscard]] TFiltersBuilder Finalize(const ui32 hashesCount, const ui32 hitsLimit) {
        std::map<ui32, std::vector<ui64>> hashesBySize;
        for (auto&& i : CountByHash) {
            hashesBySize[i.second].emplace_back(i.first);
            AFL_VERIFY(i.second);
        }
        TFiltersBuilder result;
        ui32 currentCount = 0;
        std::set<ui64> currentHashes;
        for (auto&& i : hashesBySize) {
            for (auto&& c : i.second) {
                currentCount += i.first;
                currentHashes.emplace(c);
                if (currentCount >= hitsLimit) {
                    result.AddCategory(std::move(currentHashes), currentCount, hashesCount);
                    currentCount = 0;
                    currentHashes.clear();
                }
            }
        }
        if (currentCount) {
            result.AddCategory(std::move(currentHashes), currentCount, hashesCount);
        }
        return result;
    }
};

std::vector<std::shared_ptr<IPortionDataChunk>> TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 /*recordsCount*/) const {
    std::vector<std::shared_ptr<IPortionDataChunk>> result;
    ui32 chunkIdx = 0;
    for (reader.Start(); reader.IsCorrect(); reader.ReadNext(reader.begin()->GetCurrentChunk()->GetRecordsCount())) {
        std::deque<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> dataOwners;
        TCategoriesBuilder categories;
        AFL_VERIFY(reader.GetColumnsCount() == 1);
        for (auto&& i : reader) {
            dataOwners.emplace_back(i.GetCurrentChunk());
            const THashMap<ui64, ui32> indexHitsCount = GetDataExtractor()->GetIndexHitsCount(dataOwners.back());
            for (auto&& hitInfo : indexHitsCount) {
                categories.AddHash(hitInfo.first, hitInfo.second);
            }
        }
        auto filtersBuilder = categories.Finalize(HashesCount, 1024);
        while (dataOwners.size()) {
            GetDataExtractor()->VisitAll(
                dataOwners.front(),
                [&](const std::shared_ptr<arrow::Array>& arr, const ui64 hashBase) {
                    auto& filterBits = filtersBuilder.MutableFilter(hashBase);
                    const ui32 size = filterBits.Size();
                    const auto pred = [&](const ui64 hash, const ui32 /*idx*/) {
                        filterBits.Set(hash % size);
                    };
                    for (ui64 i = 0; i < HashesCount; ++i) {
                        NArrow::NHash::TXX64::CalcForAll(arr, i, pred);
                    }
                },
                [&](const std::string_view data, const ui64 hashBase) {
                    auto& filterBits = filtersBuilder.MutableFilter(hashBase);
                    const ui32 size = filterBits.Size();
                    for (ui64 i = 0; i < HashesCount; ++i) {
                        const ui64 hash = NArrow::NHash::TXX64::CalcSimple(data, i);
                        filterBits.Set(hash % size);
                    }
                });
            dataOwners.pop_front();
        }
        NKikimrTxColumnShard::TIndexCategoriesDescription protoDescription;
        std::vector<TString> filterDescriptions;
        ui32 filtersSumSize = 0;
        for (auto&& i : filtersBuilder.MutableBuilders()) {
            filterDescriptions.emplace_back(GetBitsStorageConstructor()->Build(std::move(i.MutableFilter()))->SerializeToString());
            filtersSumSize += filterDescriptions.back().size();
            auto* category = protoDescription.AddCategories();
            category->SetFilterSize(filterDescriptions.back().size());
            for (auto&& h : i.GetCategories()) {
                category->AddHashes(h);
            }
        }
        auto protoString = protoDescription.SerializeAsString();
        TString indexData;
        {
            indexData.reserve(sizeof(ui32) + protoString.size() + filtersSumSize);
            TStringOutput so(indexData);
            const ui32 protoSize = protoString.size();
            so.Write(&protoSize, sizeof(ui32));
            so.Write(protoString.data(), protoString.size());
            for (auto&& i : filterDescriptions) {
                so.Write(i.data(), i.size());
            }
        }
        result.emplace_back(std::make_shared<NChunks::TPortionIndexChunk>(
            TChunkAddress(GetIndexId(), chunkIdx++), reader.begin()->GetCurrentChunk()->GetRecordsCount(), indexData.size(), indexData));
    }
    return result;
}

TConclusion<std::shared_ptr<IIndexHeader>> TIndexMeta::DoBuildHeader(const TChunkOriginalData& data) const {
    if (!data.HasData()) {
        return std::shared_ptr<IIndexHeader>();
    }
    auto conclusion = IIndexHeader::ReadHeader(data.GetDataVerified());
    if (conclusion.IsFail()) {
        return conclusion;
    }
    NKikimrTxColumnShard::TIndexCategoriesDescription proto;
    if (!proto.ParseFromArray(conclusion->data(), conclusion->size())) {
        return TConclusionStatus::Fail("cannot parse proto in header");
    }
    return std::make_shared<TCompositeBloomHeader>(std::move(proto), IIndexHeader::ReadHeaderSize(data.GetDataVerified(), true).DetachResult());
}

bool TIndexMeta::DoCheckValueImpl(const IBitsStorage& data, const std::optional<ui64> category, const std::shared_ptr<arrow::Scalar>& value,
    const NArrow::NSSA::TIndexCheckOperation& op) const {
    AFL_VERIFY(!!category);
    AFL_VERIFY(op.GetOperation() == EOperation::Equals)("op", op.DebugString());
    AFL_VERIFY(op.GetCaseSensitive());
    const ui32 bitsCount = data.GetBitsCount();
    if (!bitsCount) {
        return false;
    }
    for (ui64 hashSeed = 0; hashSeed < HashesCount; ++hashSeed) {
        const ui64 hash = NArrow::NHash::TXX64::CalcForScalar(value, hashSeed);
        if (!data.Get(hash % bitsCount)) {
            return false;
        }
    }
    return true;
}

std::optional<ui64> TIndexMeta::DoCalcCategory(const TString& subColumnName) const {
    ui64 result;
    const NRequest::TOriginalDataAddress addr(GetColumnId(), subColumnName);
    AFL_VERIFY(GetDataExtractor()->CheckForIndex(addr, &result));
    return result;
}

bool TIndexMeta::DoDeserializeFromProto(const NKikimrSchemeOp::TOlapIndexDescription& proto) {
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

void TIndexMeta::DoSerializeToProto(NKikimrSchemeOp::TOlapIndexDescription& proto) const {
    auto* filterProto = proto.MutableBloomFilter();
    TBase::SerializeToProtoImpl(*filterProto);
    filterProto->SetFalsePositiveProbability(FalsePositiveProbability);
    for (auto&& i : GetColumnIds()) {
        filterProto->AddColumnIds(i);
    }
    *filterProto->MutableDataExtractor() = GetDataExtractor().SerializeToProto();
}

}   // namespace NKikimr::NOlap::NIndexes::NCategoriesBloom

#include "checker.h"
#include "header.h"
#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/columnshard/engines/protos/index.pb.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/checker.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NCategoriesBloom {

class TCategory {
private:
    YDB_READONLY(ui32, Identifier, 0);
    YDB_READONLY(ui32, Size, 0);
    YDB_READONLY_DEF(std::vector<ui64>, Hashes);
    std::vector<bool> Filter;

public:
    TCategory(const ui32 id)
        : Identifier(id) {
    }

    const std::vector<bool>& GetFilter() const {
        AFL_VERIFY(Filter.size());
        return Filter;
    }

    void AddHash(const ui64 hashBase, const ui32 hitsCount) {
        Hashes.emplace_back(hashBase);
        Size += hitsCount;
    }

    void Finalize(const ui32 hashesCount) {
        AFL_VERIFY(Filter.size() == 0);
        const ui32 bitsCount = TFixStringBitsStorage::GrowBitsCountToByte(hashesCount * std::max<ui32>(Size, 10) / std::log(2));
        AFL_VERIFY(bitsCount);
        Filter.resize(bitsCount, false);
    }
};

class TCategoryBuilder {
private:
    YDB_READONLY_DEF(std::set<ui64>, Categories);
    YDB_ACCESSOR_DEF(std::vector<bool>, Filter);

public:
    TCategoryBuilder(std::set<ui64>&& categories, const ui32 count, const ui32 hashesCount)
        : Categories(categories) {
        AFL_VERIFY(count);
        const ui32 bitsCount = TFixStringBitsStorage::GrowBitsCountToByte(hashesCount * std::max<ui32>(count, 10) / std::log(2));
        AFL_VERIFY(bitsCount);
        Filter.resize(bitsCount, false);
    }
};

class TFiltersBuilder {
private:
    YDB_READONLY_DEF(std::deque<TCategoryBuilder>, Builders);
    THashMap<ui64, std::vector<bool>*> FiltersByHash;

public:
    std::vector<bool>& MutableFilter(const ui64 hashBase) {
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
        } else {
            it->second += hitsCount;
        }
    }

    [[nodiscard]] TFiltersBuilder Finalize(const ui32 hashesCount, const ui32 hitsLimit) {
        std::map<ui32, std::vector<ui64>> hashesBySize;
        for (auto&& i : CountByHash) {
            hashesBySize[i.second].emplace_back(i.first);
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

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 /*recordsCount*/) const {
    std::deque<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> dataOwners;
    TCategoriesBuilder categories;
    for (reader.Start(); reader.IsCorrect();) {
        AFL_VERIFY(reader.GetColumnsCount() == 1);
        for (auto&& i : reader) {
            dataOwners.emplace_back(i.GetCurrentChunk());
            const THashMap<ui64, ui32> indexHitsCount = GetDataExtractor()->GetIndexHitsCount(dataOwners.back());
            for (auto&& hitInfo : indexHitsCount) {
                categories.AddHash(hitInfo.first, hitInfo.second);
            }
        }
        reader.ReadNext(reader.begin()->GetCurrentChunk()->GetRecordsCount());
    }
    auto filtersBuilder = categories.Finalize(HashesCount, 1024);
    while (dataOwners.size()) {
        GetDataExtractor()->VisitAll(
            dataOwners.front(),
            [&](const std::shared_ptr<arrow::Array>& arr, const ui64 hashBase) {
                auto& filterBits = filtersBuilder.MutableFilter(hashBase);
                const auto pred = [&](const ui64 hash, const ui32 /*idx*/) {
                    filterBits[hash % filterBits.size()] = true;
                };
                for (ui64 i = 0; i < HashesCount; ++i) {
                    NArrow::NHash::TXX64::CalcForAll(arr, i, pred);
                }
            },
            [&](const std::string_view data, const ui64 hashBase) {
                auto& filterBits = filtersBuilder.MutableFilter(hashBase);
                for (ui64 i = 0; i < HashesCount; ++i) {
                    const ui64 hash = NArrow::NHash::TXX64::CalcSimple(data, i);
                    filterBits[hash % filterBits.size()] = true;
                }
            });
        dataOwners.pop_front();
    }
    NKikimrTxColumnShard::TIndexCategoriesDescription protoDescription;
    std::vector<TString> filterDescriptions;
    ui32 filtersSumSize = 0;
    for (auto&& i : filtersBuilder.GetBuilders()) {
        filterDescriptions.emplace_back(TFixStringBitsStorage(i.GetFilter()).GetData());
        filtersSumSize += filterDescriptions.back().size();
        auto* category = protoDescription.AddCategories();
        category->SetFilterSize(filterDescriptions.back().size());
        for (auto&& h : i.GetCategories()) {
            category->AddHashes(h);
        }
    }
    auto protoString = protoDescription.SerializeAsString();
    TString result;
    result.reserve(sizeof(ui32) + protoString.size() + filtersSumSize);
    TStringOutput so(result);
    const ui32 protoSize = protoString.size();
    so.Write(&protoSize, sizeof(ui32));
    so.Write(protoString.data(), protoString.size());
    for (auto&& i : filterDescriptions) {
        so.Write(i.data(), i.size());
    }
    return result;
}

void TIndexMeta::DoFillIndexCheckers(
    const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& /*schema*/) const {
    for (auto&& branch : info->GetBranches()) {
        for (auto&& i : branch->GetEquals()) {
            if (i.first.GetColumnId() != GetColumnId()) {
                continue;
            }
            ui64 hashBase = 0;
            if (!GetDataExtractor()->CheckForIndex(i.first, hashBase)) {
                continue;
            }
            std::set<ui64> hashes;
            for (ui64 hashSeed = 0; hashSeed < HashesCount; ++hashSeed) {
                const ui64 hash = NArrow::NHash::TXX64::CalcForScalar(i.second, hashSeed);
                hashes.emplace(hash);
            }
            branch->MutableIndexes().emplace_back(std::make_shared<TBloomFilterChecker>(GetIndexId(), hashBase, std::move(hashes)));
        }
    }
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

}   // namespace NKikimr::NOlap::NIndexes::NCategoriesBloom

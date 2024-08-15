#include "meta.h"
#include "checker.h"
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes {

TString TBloomIndexMeta::DoBuildIndexImpl(std::vector<TChunkedColumnReader>&& columnReaders) const {
    TChunkedBatchReader reader(std::move(columnReaders));

    std::set<ui64> hashes;
    {
        NArrow::NHash::NXX64::TStreamStringHashCalcer hashCalcer(0);
        for (reader.Start(); reader.IsCorrect(); reader.ReadNext()) {
            hashCalcer.Start();
            for (auto&& i : reader) {
                NArrow::NHash::TXX64::AppendField(i.GetCurrentChunk(), i.GetCurrentRecordIndex(), hashCalcer);
            }
            hashes.emplace(hashCalcer.Finish());
        }
    }

    const ui32 bitsCount = HashesCount * hashes.size() / std::log(2);
    TFixStringBitsStorage bits(bitsCount);
    const auto pred = [&bits](const ui64 hash) {
        bits.Set(true, hash % bits.GetSizeBits());
    };
    BuildHashesSet(hashes, pred);
    return bits.GetData();
}

void TBloomIndexMeta::DoFillIndexCheckers(const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const {
    for (auto&& branch : info->GetBranches()) {
        std::map<ui32, std::shared_ptr<arrow::Scalar>> foundColumns;
        for (auto&& cId : ColumnIds) {
            auto c = schema.GetColumns().GetById(cId);
            if (!c) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "incorrect index column")("id", cId);
                return;
            }
            auto itEqual = branch->GetEquals().find(c->GetName());
            if (itEqual == branch->GetEquals().end()) {
                break;
            }
            foundColumns.emplace(cId, itEqual->second);
        }
        if (foundColumns.size() != ColumnIds.size()) {
            continue;
        }
        std::set<ui64> hashes;
        const auto pred = [&hashes](const ui64 hash) {
            hashes.emplace(hash);
        };
        NArrow::NHash::NXX64::TStreamStringHashCalcer calcer(0);
        for (ui32 i = 0; i < HashesCount; ++i) {
            calcer.Start();
            for (auto&& i : foundColumns) {
                NArrow::NHash::TXX64::AppendField(i.second, calcer);
            }
            BuildHashesSet(calcer.Finish(), pred);
        }
        branch->MutableIndexes().emplace_back(std::make_shared<TBloomFilterChecker>(GetIndexId(), std::move(hashes)));
    }
}

}   // namespace NKikimr::NOlap::NIndexes

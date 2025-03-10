#include "meta.h"
#include "checker.h"
#include <ydb/library/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes {

TString TBloomIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const {
    const ui32 bitsCount = TFixStringBitsStorage::GrowBitsCountToByte(HashesCount * recordsCount / std::log(2));
    std::vector<bool> filterBits(bitsCount, false);
    for (ui32 i = 0; i < HashesCount; ++i) {
        NArrow::NHash::NXX64::TStreamStringHashCalcer_H3 hashCalcer(i);
        for (reader.Start(); reader.IsCorrect(); reader.ReadNext()) {
            hashCalcer.Start();
            for (auto&& i : reader) {
                NArrow::NHash::TXX64::AppendField(i.GetCurrentChunk(), i.GetCurrentRecordIndex(), hashCalcer);
            }
            filterBits[hashCalcer.Finish() % bitsCount] = true;
        }
    }

    return TFixStringBitsStorage(filterBits).GetData();
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
        for (ui32 i = 0; i < HashesCount; ++i) {
            NArrow::NHash::NXX64::TStreamStringHashCalcer_H3 calcer(i);
            calcer.Start();
            for (auto&& i : foundColumns) {
                NArrow::NHash::TXX64::AppendField(i.second, calcer);
            }
            hashes.emplace(calcer.Finish());
        }
        branch->MutableIndexes().emplace_back(std::make_shared<TBloomFilterChecker>(GetIndexId(), std::move(hashes)));
    }
}

}   // namespace NKikimr::NOlap::NIndexes

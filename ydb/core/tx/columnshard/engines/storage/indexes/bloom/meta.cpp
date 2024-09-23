#include "meta.h"
#include "checker.h"
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes {

std::shared_ptr<arrow::RecordBatch> TBloomIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader) const {
    std::set<ui64> hashes;
    for (ui32 i = 0; i < HashesCount; ++i) {
        NArrow::NHash::NXX64::TStreamStringHashCalcer hashCalcer(3 * i);
        for (reader.Start(); reader.IsCorrect(); reader.ReadNext()) {
            hashCalcer.Start();
            for (auto&& i : reader) {
                NArrow::NHash::TXX64::AppendField(i.GetCurrentChunk(), i.GetCurrentRecordIndex(), hashCalcer);
            }
            const ui64 h = hashCalcer.Finish();
            hashes.emplace(h);
        }
    }
    const ui32 bitsCount = hashes.size() / std::log(2);
    std::vector<bool> flags(bitsCount, false);
    for (auto&& i : hashes) {
        flags[i % flags.size()] = true;
    }

    arrow::BooleanBuilder builder;
    auto res = builder.Reserve(flags.size());
    NArrow::TStatusValidator::Validate(builder.AppendValues(flags));
    std::shared_ptr<arrow::BooleanArray> out;
    NArrow::TStatusValidator::Validate(builder.Finish(&out));

    return arrow::RecordBatch::Make(ResultSchema, bitsCount, {out});
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
            NArrow::NHash::NXX64::TStreamStringHashCalcer calcer(3 * i);
            calcer.Start();
            for (auto&& i : foundColumns) {
                NArrow::NHash::TXX64::AppendField(i.second, calcer);
            }
            const ui64 hash = calcer.Finish();
            hashes.emplace(hash);
        }
        branch->MutableIndexes().emplace_back(std::make_shared<TBloomFilterChecker>(GetIndexId(), std::move(hashes)));
    }
}

}   // namespace NKikimr::NOlap::NIndexes
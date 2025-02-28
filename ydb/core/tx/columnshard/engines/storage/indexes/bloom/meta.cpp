#include "checker.h"
#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes {

TString TBloomIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 recordsCount) const {
    const ui32 bitsCount = TFixStringBitsStorage::GrowBitsCountToByte(HashesCount * recordsCount / std::log(2));
    std::vector<bool> filterBits(bitsCount, false);
    for (reader.Start(); reader.IsCorrect();) {
        AFL_VERIFY(reader.GetColumnsCount() == 1);
        for (auto&& i : reader) {
            GetDataExtractor()->VisitAll(
                i.GetCurrentChunk(),
                [&](const std::shared_ptr<arrow::Array>& arr, const ui64 hashBase) {
                    for (ui32 idx = 0; idx < arr->length(); ++idx) {
                        for (ui32 i = 0; i < HashesCount; ++i) {
                            NArrow::NHash::NXX64::TStreamStringHashCalcer_H3 hashCalcer(i);
                            hashCalcer.Start();
                            if (hashBase) {
                                hashCalcer.Update((const ui8*)&hashBase, sizeof(hashBase));
                            }
                            NArrow::NHash::TXX64::AppendField(arr, idx, hashCalcer);
                            filterBits[hashCalcer.Finish() % bitsCount] = true;
                        }
                    }
                },
                [&](const std::string_view data, const ui64 hashBase) {
                    for (ui32 i = 0; i < HashesCount; ++i) {
                        NArrow::NHash::NXX64::TStreamStringHashCalcer_H3 hashCalcer(i);
                        hashCalcer.Start();
                        if (hashBase) {
                            hashCalcer.Update((const ui8*)&hashBase, sizeof(hashBase));
                        }
                        hashCalcer.Update((const ui8*)data.data(), data.size());
                        filterBits[hashCalcer.Finish() % bitsCount] = true;
                    }
                });
        }
        reader.ReadNext(reader.begin()->GetCurrentChunk()->GetRecordsCount());
    }

    return TFixStringBitsStorage(filterBits).GetData();
}

void TBloomIndexMeta::DoFillIndexCheckers(
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
            for (ui32 hashSeed = 0; hashSeed < HashesCount; ++hashSeed) {
                NArrow::NHash::NXX64::TStreamStringHashCalcer_H3 calcer(hashSeed);
                calcer.Start();
                if (hashBase) {
                    calcer.Update((const ui8*)&hashBase, sizeof(hashBase));
                }
                NArrow::NHash::TXX64::AppendField(i.second, calcer);
                hashes.emplace(calcer.Finish());
            }
            branch->MutableIndexes().emplace_back(std::make_shared<TBloomFilterChecker>(GetIndexId(), std::move(hashes)));
        }
    }
}

}   // namespace NKikimr::NOlap::NIndexes

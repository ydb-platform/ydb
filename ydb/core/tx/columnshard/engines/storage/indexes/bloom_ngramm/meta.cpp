#include "checker.h"
#include "meta.h"

#include <ydb/core/formats/arrow/hash/calcer.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bloom/checker.h>
#include <ydb/core/tx/program/program.h>
#include <ydb/core/tx/schemeshard/olap/schema/schema.h>

#include <ydb/library/formats/arrow/hash/xx_hash.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr::NOlap::NIndexes::NBloomNGramm {

class TNGrammBuilder {
private:
    NArrow::NHash::NXX64::TStreamStringHashCalcer HashCalcer;
    TBuffer Zeros;
    template <class TAction>
    void BuildNGramms(const char* data, const ui32 dataSize, const std::optional<NRequest::TLikePart::EOperation> op, const ui32 nGrammSize,
        const TAction& pred) const {
        if (!op || op == NRequest::TLikePart::EOperation::StartsWith) {
            for (ui32 c = 1; c <= nGrammSize; ++c) {
                TBuffer fakeStart;
                fakeStart.Fill('\0', nGrammSize - c);
                fakeStart.Append(data, std::min(c, dataSize));
                if (fakeStart.size() < nGrammSize) {
                    fakeStart.Append(Zeros.data(), nGrammSize - fakeStart.size());
                }
                pred(fakeStart.data());
            }
        }
        for (ui32 c = 0; c < dataSize; ++c) {
            if (c + nGrammSize <= dataSize) {
                pred(data + c);
            } else if (!op || op == NRequest::TLikePart::EOperation::EndsWith) {
                TBuffer fakeStart;
                fakeStart.Append(data + c, dataSize - c);
                fakeStart.Append(Zeros.data(), nGrammSize - fakeStart.size());
                pred(fakeStart.data());
            }
        }
    }

public:
    TNGrammBuilder()
        : HashCalcer(0) {
        Zeros.Fill('\0', 1024);
    }

    template <class TFiller>
    void FillNGrammHashes(const ui32 nGrammSize, const std::shared_ptr<arrow::Array>& array, const TFiller& fillData) {
        AFL_VERIFY(array->type_id() == arrow::utf8()->id())("id", array->type()->ToString());
        NArrow::SwitchType(array->type_id(), [&](const auto& type) {
            using TWrap = std::decay_t<decltype(type)>;
            using T = typename TWrap::T;
            using TArray = typename arrow::TypeTraits<T>::ArrayType;
            auto& typedArray = static_cast<const TArray&>(*array);

            for (ui32 row = 0; row < array->length(); ++row) {
                if (array->IsNull(row)) {
                    continue;
                }
                if constexpr (arrow::has_string_view<T>()) {
                    auto value = typedArray.GetView(row);
                    if (value.size() < nGrammSize) {
                        continue;
                    }
                    const auto pred = [&](const char* data) {
                        HashCalcer.Start();
                        HashCalcer.Update((const ui8*)data, nGrammSize);
                        fillData(HashCalcer.Finish());
                    };
                    BuildNGramms(value.data(), value.size(), {}, nGrammSize, pred);
                } else {
                    AFL_VERIFY(false);
                }
            }
            return true;
        });
    }

    template <class TFiller>
    void FillNGrammHashes(const ui32 nGrammSize, const NRequest::TLikePart::EOperation op, const TString& userReq, const TFiller& fillData) {
        const auto pred = [&](const char* value) {
            HashCalcer.Start();
            HashCalcer.Update((const ui8*)value, nGrammSize);
            fillData(HashCalcer.Finish());
        };
        BuildNGramms(userReq.data(), userReq.size(), op, nGrammSize, pred);
    }
};

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader) const {
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    TNGrammBuilder builder;

    TFixStringBitsStorage bits(FilterSizeBytes * 8);

    const auto pred = [&](const ui64 hash) {
        const auto predSet = [&](const ui64 hashSecondary) {
            bits.Set(true, hashSecondary % bits.GetSizeBits());
        };
        BuildHashesSet(hash, predSet);
    };
    for (reader.Start(); reader.IsCorrect();) {
        builder.FillNGrammHashes(NGrammSize, reader.begin()->GetCurrentChunk(), pred);
        reader.ReadNext(reader.begin()->GetCurrentChunk()->length());
    }

    return bits.GetData();
}

void TIndexMeta::DoFillIndexCheckers(
    const std::shared_ptr<NRequest::TDataForIndexesCheckers>& info, const NSchemeShard::TOlapSchema& schema) const {
    for (auto&& branch : info->GetBranches()) {
        std::map<ui32, NRequest::TLikeDescription> foundColumns;
        for (auto&& cId : ColumnIds) {
            auto c = schema.GetColumns().GetById(cId);
            if (!c) {
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "incorrect index column")("id", cId);
                return;
            }
            auto it = branch->GetLikes().find(c->GetName());
            if (it == branch->GetLikes().end()) {
                break;
            }
            foundColumns.emplace(cId, it->second);
        }
        if (foundColumns.size() != ColumnIds.size()) {
            continue;
        }

        std::set<ui64> hashes;
        const auto pred = [&](const ui64 hash) {
            const auto predSet = [&](const ui64 hashSecondary) {
                hashes.emplace(hashSecondary);
            };
            BuildHashesSet(hash, predSet);
        };
        TNGrammBuilder builder;
        for (auto&& c : foundColumns) {
            for (auto&& ls : c.second.GetLikeSequences()) {
                builder.FillNGrammHashes(NGrammSize, ls.second.GetOperation(), ls.second.GetValue(), pred);
            }
        }
        branch->MutableIndexes().emplace_back(std::make_shared<TFilterChecker>(GetIndexId(), std::move(hashes)));
    }
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm

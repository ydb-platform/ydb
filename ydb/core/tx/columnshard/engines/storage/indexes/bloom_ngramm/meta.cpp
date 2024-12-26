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
    const ui32 HashesCount;

    template <int HashIdx>
    class THashesBuilder {
    public:
        template <class TActor>
        static void Build(const ui8* data, ui64& h, const TActor& actor) {
            h = h ^ uint64_t(*data);
            h = h * 16777619;
            THashesBuilder<HashIdx - 1>::Build(data + 1, h, actor);
        }
    };

    template <>
    class THashesBuilder<0> {
    public:
        template <class TActor>
        static void Build(const ui8* /*data*/, ui64& hash, const TActor& actor) {
            actor(hash);
        }
    };

    template <class TActor>
    void BuildHashesSet(const ui8* data, const ui32 dataSize, const TActor& actor) {
        for (ui32 i = 1; i <= HashesCount; ++i) {
            ui64 hash = 2166136261 * i;
            if (dataSize == 3) {
                THashesBuilder<3>::Build(data, hash, actor);
            } else if (dataSize == 4) {
                THashesBuilder<4>::Build(data, hash, actor);
            } else if (dataSize == 5) {
                THashesBuilder<5>::Build(data, hash, actor);
            } else if (dataSize == 6) {
                THashesBuilder<6>::Build(data, hash, actor);
            } else if (dataSize == 7) {
                THashesBuilder<7>::Build(data, hash, actor);
            } else if (dataSize == 8) {
                THashesBuilder<8>::Build(data, hash, actor);
            } else {
                NArrow::NHash::NXX64::TStreamStringHashCalcer calcer(i);
                calcer.Start();
                calcer.Update(data, dataSize);
                actor(calcer.Finish());
            }
        }
    }

    template <class TAction>
    void BuildNGramms(const char* data, const ui32 dataSize, const std::optional<NRequest::TLikePart::EOperation> op, const ui32 nGrammSize,
        const TAction& pred) {
        TBuffer fakeString;
        AFL_VERIFY(nGrammSize >= 3)("value", nGrammSize);
        if (!op || op == NRequest::TLikePart::EOperation::StartsWith) {
            for (ui32 c = 1; c <= nGrammSize; ++c) {
                fakeString.Clear();
                fakeString.Fill('\0', nGrammSize - c);
                fakeString.Append(data, std::min(c, dataSize));
                if (fakeString.size() < nGrammSize) {
                    fakeString.Fill('\0', nGrammSize - fakeString.size());
                }
                BuildHashesSet((const ui8*)fakeString.data(), nGrammSize, pred);
            }
        }
        ui32 c = 0;
        for (; c + nGrammSize <= dataSize; ++c) {
            BuildHashesSet((const ui8*)(data + c), nGrammSize, pred);
        }

        if (!op || op == NRequest::TLikePart::EOperation::EndsWith) {
            for (; c < dataSize; ++c) {
                fakeString.Clear();
                fakeString.Append(data + c, dataSize - c);
                fakeString.Fill('\0', nGrammSize - fakeString.size());
                BuildHashesSet((const ui8*)fakeString.data(), nGrammSize, pred);
            }
        }
    }

public:
    TNGrammBuilder(const ui32 hashesCount)
        : HashesCount(hashesCount) {
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
                    BuildNGramms(value.data(), value.size(), {}, nGrammSize, fillData);
                } else {
                    AFL_VERIFY(false);
                }
            }
            return true;
        });
    }

    template <class TFiller>
    void FillNGrammHashes(const ui32 nGrammSize, const NRequest::TLikePart::EOperation op, const TString& userReq, const TFiller& fillData) {
        BuildNGramms(userReq.data(), userReq.size(), op, nGrammSize, fillData);
    }
};

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader, const ui32 /*recordsCount*/) const {
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    TNGrammBuilder builder(HashesCount);

    std::vector<bool> bitsVector(FilterSizeBytes * 8, false);
    bool* memAccessor = &bitsVector[0];
    const auto predSet = [&](const ui64 hashSecondary) {
        memAccessor[hashSecondary % (FilterSizeBytes * 8)] = true;
    };
    for (reader.Start(); reader.IsCorrect();) {
        builder.FillNGrammHashes(NGrammSize, reader.begin()->GetCurrentChunk(), predSet);
        reader.ReadNext(reader.begin()->GetCurrentChunk()->length());
    }
    return TFixStringBitsStorage(bitsVector).GetData();
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
        const auto predSet = [&](const ui64 hashSecondary) {
            hashes.emplace(hashSecondary);
        };
        TNGrammBuilder builder(HashesCount);
        for (auto&& c : foundColumns) {
            for (auto&& ls : c.second.GetLikeSequences()) {
                builder.FillNGrammHashes(NGrammSize, ls.second.GetOperation(), ls.second.GetValue(), predSet);
            }
        }
        branch->MutableIndexes().emplace_back(std::make_shared<TFilterChecker>(GetIndexId(), std::move(hashes)));
    }
}

}   // namespace NKikimr::NOlap::NIndexes::NBloomNGramm

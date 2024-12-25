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
    TBuffer Zeros;
    const ui32 HashesCount;

    static const ui64 HashesConstructorP = 9223372036854775783;
    static const ui64 HashesConstructorA = 1;

    template <int HashIdx>
    class THashesBuilder {
    public:
        template <class TActor>
        static void Build(const ui64 originalHash, const TActor& actor) {
            actor((HashesConstructorA * originalHash + HashIdx) % HashesConstructorP);
        }
    };

    template <>
    class THashesBuilder<0> {
    public:
        template <class TActor>
        static void Build(const ui64 /*originalHash*/, const TActor& /*actor*/) {
        }
    };

    template <class TActor>
    void BuildHashesSet(const ui64 originalHash, const TActor& actor) const {
        if (HashesCount == 1) {
            THashesBuilder<1>::Build(originalHash, actor);
        } else if (HashesCount == 2) {
            THashesBuilder<2>::Build(originalHash, actor);
        } else if (HashesCount == 3) {
            THashesBuilder<3>::Build(originalHash, actor);
        } else if (HashesCount == 4) {
            THashesBuilder<4>::Build(originalHash, actor);
        } else if (HashesCount == 5) {
            THashesBuilder<5>::Build(originalHash, actor);
        } else if (HashesCount == 6) {
            THashesBuilder<6>::Build(originalHash, actor);
        } else if (HashesCount == 7) {
            THashesBuilder<7>::Build(originalHash, actor);
        } else if (HashesCount == 8) {
            THashesBuilder<8>::Build(originalHash, actor);
        } else {
            for (ui32 b = 1; b <= HashesCount; ++b) {
                const ui64 hash = (HashesConstructorA * originalHash + b) % HashesConstructorP;
                actor(hash);
            }
        }
    }

    ui64 CalcHash(const char* data, const ui32 size) const {
        if (size == 3) {
            return ((ui64)data[0]) | (((ui64)data[1]) << 8) | (((ui64)data[2]) << 16);
        } else if (size == 4) {
            return *(ui32*)&data[0];
        } else {
            uint64_t h = 2166136261;
            for (size_t i = 0; i < size; i++) {
                h = h ^ uint64_t(data[i]);
                h = h * 16777619;
            }
            return h;
        }
    }

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
                BuildHashesSet(CalcHash(fakeStart.data(), nGrammSize), pred);
            }
        }
        for (ui32 c = 0; c < dataSize; ++c) {
            if (c + nGrammSize <= dataSize) {
                pred(CalcHash(data + c, nGrammSize));
            } else if (!op || op == NRequest::TLikePart::EOperation::EndsWith) {
                TBuffer fakeStart;
                fakeStart.Append(data + c, dataSize - c);
                fakeStart.Append(Zeros.data(), nGrammSize - fakeStart.size());
                BuildHashesSet(CalcHash(fakeStart.data(), nGrammSize), pred);
            }
        }
    }

public:
    TNGrammBuilder(const ui32 hashesCount)
        : HashesCount(hashesCount)
    {
        AFL_VERIFY((ui64)HashesCount < HashesConstructorP);
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

TString TIndexMeta::DoBuildIndexImpl(TChunkedBatchReader& reader) const {
    AFL_VERIFY(reader.GetColumnsCount() == 1)("count", reader.GetColumnsCount());
    TNGrammBuilder builder(HashesCount);

    std::vector<bool> bitsVector(FilterSizeBytes * 8, false);
    const auto predSet = [&](const ui64 hashSecondary) {
        bitsVector[hashSecondary % (FilterSizeBytes * 8)] = true;
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

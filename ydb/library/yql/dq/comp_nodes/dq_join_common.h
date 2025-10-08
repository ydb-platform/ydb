#pragma once
#include "dq_hash_join_table.h"
#include <ranges>
#include <vector>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL {
struct TColumnsMetadata {
    std::vector<ui32> KeyColumns;
    std::vector<TType*> ColumnTypes;
};

struct TJoinMetadata {
    TColumnsMetadata Build;
    TColumnsMetadata Probe;
    TKeyTypes KeyTypes;
};

class TBlockRowSource {
  public:
    TBlockRowSource(TComputationContext& ctx, IComputationNode* stream, const std::vector<TType*>& types)
        : Stream_(stream)
        , Values_(Stream_->GetValue(ctx))
        , Buff_(types.size() + 1)
    {
        TTypeInfoHelper typeInfoHelper;
        for (int index = 0; index < std::ssize(types); ++index) {
            InputReaders[index] = NYql::NUdf::MakeBlockReader(typeInfoHelper, types[index]);
            InputItemConverters[index] =
                MakeBlockItemConverter(typeInfoHelper, types[index], ctx.Builder->GetPgBuilder());
        }
    }

    bool Finished() const {
        return Finished_;
    }

    int Size() const {
        return ConsumeBuff_.size();
    }

    NYql::NUdf::EFetchStatus ForEachRow(TComputationContext& ctx, auto consume) {
        auto res = Values_.WideFetch(Buff_.data(), Buff_.size());
        if (res != NYql::NUdf::EFetchStatus::Ok) {
            if (res == NYql::NUdf::EFetchStatus::Finish) {
                Finished_ = true;
            }
            return res;
        }
        const int cols = std::ssize(Buff_) - 1;

        for (int index = 0; index < cols; ++index) {
            Blocks_[index] = &TArrowBlock::From(Buff_[index]).GetDatum();
        }

        const int rows = TArrowBlock::From(Buff_.back()).GetDatum().scalar_as<arrow::UInt64Scalar>().value;

        for (int rowIndex = 0; rowIndex < rows; ++rowIndex) {
            for (int colIndex = 0; colIndex < cols; ++colIndex) {
                ConsumeBuff_[colIndex] = InputItemConverters[colIndex]->MakeValue(
                    InputReaders[colIndex]->GetItem(*Blocks_[colIndex]->array(), rowIndex), ctx.HolderFactory);
            }
            consume(ConsumeBuff_.data());
        }
        return NYql::NUdf::EFetchStatus::Ok;
    }

  private:
    bool Finished_ = false;
    IComputationNode* Stream_;
    NYql::NUdf::TUnboxedValue Values_;
    TUnboxedValueVector Buff_;
    TUnboxedValueVector ConsumeBuff_{Buff_.size() - 1};
    std::vector<const arrow::Datum*> Blocks_{Buff_.size() - 1};
    std::vector<std::unique_ptr<IBlockReader>> InputReaders{Buff_.size() - 1};
    std::vector<std::unique_ptr<IBlockItemConverter>> InputItemConverters{Buff_.size() - 1};
};

template <EJoinKind Kind> struct TJoinKindTag {
    static constexpr EJoinKind Kind_ = Kind;
};

using TTypedJoinKind =
    std::variant<TJoinKindTag<EJoinKind::Inner>, TJoinKindTag<EJoinKind::Full>, TJoinKindTag<EJoinKind::Left>,
                 TJoinKindTag<EJoinKind::Right>, TJoinKindTag<EJoinKind::LeftOnly>, TJoinKindTag<EJoinKind::RightOnly>,
                 TJoinKindTag<EJoinKind::LeftSemi>, TJoinKindTag<EJoinKind::RightSemi>,
                 TJoinKindTag<EJoinKind::Exclusion>, TJoinKindTag<EJoinKind::Cross>, TJoinKindTag<EJoinKind::SemiSide>,
                 TJoinKindTag<EJoinKind::SemiMask>>;

TTypedJoinKind TypifyJoinKind(EJoinKind kind);

TKeyTypes KeyTypesFromColumns(const std::vector<TType*>& types, const std::vector<ui32>& keyIndexes);

constexpr bool SemiOrOnlyJoin(EJoinKind kind) {
    switch (kind) {
        using enum EJoinKind;
    case RightOnly:
    case RightSemi:
    case LeftOnly:
    case LeftSemi:
        return true;
    default:
        return false;
    }
}

constexpr bool IsInner(EJoinKind kind) {
    switch (kind) {
        using enum EJoinKind;
    case Inner:
    case Full:
    case Left:
    case Right:
        return true;
    default:
        return false;
    }
}

template <typename Source, EJoinKind Kind> class TJoin : public TComputationValue<TJoin<Source, Kind>> {
    using TBase = TComputationValue<TJoin>;

    // void AppendTuple(NJoinTable::TTuple left, NJoinTable::TTuple right, std::vector<NUdf::TUnboxedValue>& output) {
    //     MKQL_ENSURE(left || right,"appending invalid tuple");
    //     auto outIt = std::back_inserter(output);
    //     if (left) {
    //         std::copy_n(left,LeftSize(), outIt);
    //     } else {
    //         std::copy_n(NullTuples.data(),LeftSize(), outIt);
    //     }
    //     if (right) {
    //         std::copy_n(right,RightSize(), outIt);
    //     } else {
    //         std::copy_n(NullTuples.data(),RightSize(), outIt);
    //     }
    // }

  public:
    TJoin(TMemoryUsageInfo* memInfo, Source probe, Source build, TJoinMetadata meta, NUdf::TLoggerPtr logger,
          TString componentName)
        : TBase(memInfo)
        , Meta_(meta)
        , Logger_(logger)
        , LogComponent_(logger->RegisterComponent(componentName))
        , Build_(std::move(build))
        , Probe_(std::move(probe))
        , Table_(BuildSize(), TWideUnboxedEqual{Meta_.KeyTypes}, TWideUnboxedHasher{Meta_.KeyTypes},
                 NJoinTable::NeedToTrackUnusedRightTuples(Kind))
    {
        MKQL_ENSURE(BuildSize() == ProbeSize(), "unimplemented");
        MKQL_ENSURE(Kind != EJoinKind::Cross, "Unsupported join kind");
        UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Debug, "TScalarHashJoinState created");
    }

    const TJoinMetadata& Meta() const {
        return Meta_;
    }

    int ProbeSize() const {
        return Meta_.Probe.ColumnTypes.size();
    }

    int BuildSize() const {
        return Meta_.Build.ColumnTypes.size();
    }

    EFetchResult MatchRows(TComputationContext& ctx, auto consume) {
        // const int outputTupleSize = [&] {
        //     if (SemiOrOnlyJoin(JoinKind())) {
        //         if (JoinKind() == EJoinKind::RightOnly || JoinKind() == EJoinKind::RightSemi) {
        //             return RightSize();
        //         } else {
        //             return LeftSize();
        //         }
        //     } else {
        //         return RightSize() + LeftSize();
        //     }
        // }();
        if (!Build_.Finished()) {
            auto res = Build_.ForEachRow(ctx, [&](auto tuple) { Table_.Add({tuple, tuple + Build_.Size()}); });
            switch (res) {
            case NYql::NUdf::EFetchStatus::Finish: {
                Table_.Build();
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                return EFetchResult::Yield;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        if (!Probe_.Finished()) {
            auto result = Probe_.ForEachRow(ctx, [&](NJoinTable::TTuple probeTuple) {
                bool found = false;
                Table_.Lookup(probeTuple, [&](NJoinTable::TTuple matchedBuildTuple) {
                    if constexpr (IsInner(Kind)) {
                        consume(probeTuple, matchedBuildTuple);
                    }
                    found = true;
                });
                if (!found) {
                    if constexpr (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Left || Kind == EJoinKind::Full) {
                        consume(probeTuple, nullptr);
                    }
                    if constexpr (Kind == EJoinKind::LeftOnly) {
                        consume(probeTuple);
                    }
                }
                if constexpr (Kind == EJoinKind::LeftSemi) {
                    if (found) {
                        consume(probeTuple);
                    }
                }
            });
            switch (result) {
            case NYql::NUdf::EFetchStatus::Finish: {
                if (Table_.UnusedTrackingOn()) {
                    if constexpr (Kind == EJoinKind::RightSemi) {
                        for (auto& v : Table_.MapView()) {
                            if (v.second.Used) {
                                for (NJoinTable::TTuple used : v.second.Tuples) {
                                    consume(used);
                                }
                            }
                        }
                    }
                    Table_.ForEachUnused([&](NJoinTable::TTuple unused) {
                        if constexpr (Kind == EJoinKind::RightOnly) {
                            consume(unused);
                        }
                        if constexpr (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Right ||
                                      Kind == EJoinKind::Full) {
                            consume(nullptr, unused);
                        }
                    });
                }
                return EFetchResult::One;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                return EFetchResult::One;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        return EFetchResult::Finish;
    }

  private:
    const TJoinMetadata Meta_;
    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;

    Source Build_;
    Source Probe_;
    NJoinTable::TStdJoinTable Table_;
};

} // namespace NKikimr::NMiniKQL
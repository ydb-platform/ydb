#pragma once
#include "dq_hash_join_table.h"
#include <vector>
#include <ydb/library/yql/dq/comp_nodes/hash_join_utils/block_layout_converter.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL {
struct TColumnsMetadata {
    std::vector<ui32> KeyColumns;
    std::vector<TType*> ColumnTypes;
};

enum class ESide { Probe, Build };

template <typename T> struct TSides {
    T Build;
    T Probe;

    T& SelectSide(ESide side) {
        return side == ESide::Build ? Build : Probe;
    }

    const T& SelectSide(ESide side) const {
        return side == ESide::Build ? Build : Probe;
    }
};


/*
  usage:
  instead of copy pasting code and changing "build" to "probe", use TSides and ForEachSide to call same code twice.
  example:

void f(int buildSize, int probeSize, bool buildRequired, bool probeRequired){
    use(buildSize + (buildRequired ? 0 : transform(buildSize)));
    use(probeSize + (probeRequired ? 0 : transform(probeSize)));
}

vs

void f(TSides<int> sizes, TSides<bool> required) {
    ForEachSide([&](ESide side){
        use(sizes.SelectSide(side) + (required.SelectSide(side) ? 0 : transform(sizes.SelectSide(side))));
    });
}
  
*/
void ForEachSide(std::invocable<ESide> auto fn) {
    fn(ESide::Build);
    fn(ESide::Probe);
}

struct TJoinMetadata {
    TColumnsMetadata Build;
    TColumnsMetadata Probe;
    TKeyTypes KeyTypes;
};

TKeyTypes KeyTypesFromColumns(const std::vector<TType*>& types, const std::vector<ui32>& keyIndexes);

template <EJoinKind Kind> struct TRenamedOutput {
    TRenamedOutput(TDqUserRenames renames, const std::vector<TType*>& leftColumnTypes,
                   const std::vector<TType*>& rightColumnTypes)
        : OutputBuffer()
        , NullTuples(std::max(leftColumnTypes.size(), rightColumnTypes.size()), NYql::NUdf::TUnboxedValuePod{})
        , Renames(std::move(renames))
    {}

    int TupleSize() const {
        return Renames.size();
    }

    int SizeTuples() const {
        MKQL_ENSURE(OutputBuffer.size() % TupleSize() == 0, "buffer contains tuple parts??");
        return OutputBuffer.size() / TupleSize();
    }

    std::vector<NUdf::TUnboxedValue> OutputBuffer;

    auto MakeConsumeFn() {
        return [&] {
            if constexpr (SemiOrOnlyJoin(Kind)) {
                return [&](NJoinTable::TTuple tuple) {
                    MKQL_ENSURE(tuple != nullptr, "null output row in semi/only join?");
                    for (int index = 0; index < std::ssize(Renames); ++index) {
                        auto thisRename = Renames[index];
                        OutputBuffer.push_back(tuple[thisRename.Index]);
                    }
                };
            } else {
                return [&](NJoinTable::TTuple probe, NJoinTable::TTuple build) {
                    if (!probe) { // todo: remove nullptr checks for some join types.
                        probe = NullTuples.data();
                    }

                    if (!build) {
                        build = NullTuples.data();
                    }
                    for (int index = 0; index < std::ssize(Renames); ++index) {
                        auto thisRename = Renames[index];
                        if (thisRename.Side == EJoinSide::kLeft) {
                            OutputBuffer.push_back(probe[thisRename.Index]);
                        } else {
                            OutputBuffer.push_back(build[thisRename.Index]);
                        }
                    }
                };
            }
        }();
    }

  private:
    const std::vector<NYql::NUdf::TUnboxedValue> NullTuples;
    const TDqUserRenames Renames;
};

// Some joins produce concatenation of 2 tuples, some produce one tuple(effectively)
template <typename Fun, typename Tuple>
concept JoinMatchFun = std::invocable<Fun, NJoinTable::TTuple> || std::invocable<Fun, TSides<Tuple>>;

IBlockLayoutConverter::TPackResult Flatten(std::vector<IBlockLayoutConverter::TPackResult> tuples);

template <typename Source, EJoinKind Kind> class TJoin : public TComputationValue<TJoin<Source, Kind>> {
    using TBase = TComputationValue<TJoin>;

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
        return Probe_.UserDataSize();
    }

    int BuildSize() const {
        return Build_.UserDataSize();
    }

    EFetchResult MatchRows(TComputationContext& ctx, auto consumeOneOrTwoTuples) {
        while (!Build_.Finished()) {
            auto res = Build_.ForEachRow(ctx, [&](auto tuple) { Table_.Add({tuple, tuple + Build_.UserDataSize()}); });
            switch (res) {
            case NYql::NUdf::EFetchStatus::Finish: {
                Table_.Build();
                break;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                break;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        if (!Probe_.Finished()) {
            auto result = Probe_.ForEachRow(ctx, [&](NJoinTable::TTuple probeTuple) {
                bool found = false;
                Table_.Lookup(probeTuple, [&](NJoinTable::TTuple matchedBuildTuple) {
                    if constexpr (ContainsRowsFromInnerJoin(Kind)) {
                        consumeOneOrTwoTuples(probeTuple, matchedBuildTuple);
                    }
                    found = true;
                });
                if (!found) {
                    if constexpr (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Left || Kind == EJoinKind::Full) {
                        consumeOneOrTwoTuples(probeTuple, nullptr);
                    }
                    if constexpr (Kind == EJoinKind::LeftOnly) {
                        consumeOneOrTwoTuples(probeTuple);
                    }
                }
                if constexpr (Kind == EJoinKind::LeftSemi) {
                    if (found) {
                        consumeOneOrTwoTuples(probeTuple);
                    }
                }
            });
            switch (result) {
            case NYql::NUdf::EFetchStatus::Finish: {
                int consumedTotal = 0;
                if (Table_.UnusedTrackingOn()) {
                    if constexpr (Kind == EJoinKind::RightSemi) {
                        for (auto& v : Table_.MapView()) {
                            if (v.second.Used) {
                                for (NJoinTable::TTuple used : v.second.Tuples) {

                                    ++consumedTotal;
                                    consumeOneOrTwoTuples(used);
                                }
                            }
                        }
                    }
                    Table_.ForEachUnused([&](NJoinTable::TTuple unused) {
                        if constexpr (Kind == EJoinKind::RightOnly) {
                            ++consumedTotal;
                            consumeOneOrTwoTuples(unused);
                        }
                        if constexpr (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Right ||
                                      Kind == EJoinKind::Full) {
                            ++consumedTotal;
                            consumeOneOrTwoTuples(nullptr, unused);
                        }
                    });
                }
                return consumedTotal == 0 ? EFetchResult::Finish : EFetchResult::One;
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

template <typename Source> class TJoinPackedTuples {
  public:
    using TTable = NJoinTable::TNeumannJoinTable;

    TJoinPackedTuples(TSides<Source> sources, NUdf::TLoggerPtr logger, TString componentName,
                      TSides<const NPackedTuple::TTupleLayout*> layouts)
        : Logger_(logger)
        , LogComponent_(logger->RegisterComponent(componentName))
        , Sources_(std::move(sources))
        , Layouts_(layouts)
        , Table_(Layouts_.Build)
    {}

    IBlockLayoutConverter::TPackResult Flatten(std::vector<IBlockLayoutConverter::TPackResult> tuples) {
        IBlockLayoutConverter::TPackResult flattened;
        flattened.NTuples = std::accumulate(tuples.begin(), tuples.end(), i64{0},
                                            [](i64 summ, const auto& packRes) { return summ += packRes.NTuples; });

        i64 totalTuplesSize = std::accumulate(tuples.begin(), tuples.end(), i64{0}, [](i64 summ, const auto& packRes) {
            return summ += std::ssize(packRes.PackedTuples);
        });
        flattened.PackedTuples.reserve(totalTuplesSize);

        i64 totaOverflowlSize =
            std::accumulate(tuples.begin(), tuples.end(), i64{0},
                            [](i64 summ, const auto& packRes) { return summ += std::ssize(packRes.Overflow); });
        flattened.Overflow.reserve(totaOverflowlSize);

        int tupleSize = Layouts_.Build->TotalRowSize;
        for (const IBlockLayoutConverter::TPackResult& tupleBatch : tuples) {
            Layouts_.Build->Concat(flattened.PackedTuples, flattened.Overflow,
                                   std::ssize(flattened.PackedTuples) / tupleSize, tupleBatch.PackedTuples.data(),
                                   tupleBatch.Overflow.data(), tupleBatch.PackedTuples.size() / tupleSize,
                                   tupleBatch.Overflow.size());
        }
        return flattened;
    }

    EFetchResult MatchRows([[maybe_unused]] TComputationContext& ctx,
                           JoinMatchFun<TTable::Tuple> auto consumeOneOrTwoTuples) {
        while (!Sources_.Build.Finished()) {
            FetchResult<IBlockLayoutConverter::TPackResult> var = Sources_.Build.FetchRow();
            switch (AsStatus(var)) {
            case NYql::NUdf::EFetchStatus::Finish: {
                Table_.BuildWith(Flatten(BuildChunks_));
                break;
            }
            case NYql::NUdf::EFetchStatus::Yield: {
                return EFetchResult::Yield;
            }
            case NYql::NUdf::EFetchStatus::Ok: {
                auto& packResult = std::get<One<IBlockLayoutConverter::TPackResult>>(var);
                BuildChunks_.push_back(std::move(packResult.Data));
                break;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        if (Table_.Empty()) {
            return EFetchResult::Finish; // is it ok?
        }

        if (!Sources_.Probe.Finished()) {
            const FetchResult<IBlockLayoutConverter::TPackResult> var = Sources_.Probe.FetchRow();
            const NKikimr::NMiniKQL::EFetchResult resEnum = AsResult(var);

            if (resEnum == EFetchResult::One) {
                const IBlockLayoutConverter::TPackResult& thisPackResult =
                    std::get<One<IBlockLayoutConverter::TPackResult>>(var).Data;
                for (int index = 0; index < thisPackResult.NTuples; ++index) {
                    const ui8* thisRow = &thisPackResult.PackedTuples[index * Layouts_.Probe->TotalRowSize];
                    TTable::Tuple probeRow{thisRow, thisPackResult.Overflow.data()};
                    Table_.Lookup(probeRow, [&](TTable::Tuple matchedBuildRow) {
                        consumeOneOrTwoTuples(TSides<TTable::Tuple>{.Build = matchedBuildRow, .Probe = probeRow});
                    });
                }
            }

            return resEnum;
        }

        return EFetchResult::Finish;
    }

  private:
    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
    TSides<Source> Sources_;
    TSides<const NPackedTuple::TTupleLayout*> Layouts_;
    TTable Table_;
    IBlockLayoutConverter::TPackResult BuildData_;
    std::vector<IBlockLayoutConverter::TPackResult> BuildChunks_;
};

} // namespace NKikimr::NMiniKQL
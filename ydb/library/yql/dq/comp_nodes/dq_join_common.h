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

template <EJoinKind Kind> struct TJoinKindTag {
    static constexpr EJoinKind Kind_ = Kind;
};

using TTypedJoinKind =
    std::variant<TJoinKindTag<EJoinKind::Inner>, 
                 TJoinKindTag<EJoinKind::Left>,
                 TJoinKindTag<EJoinKind::Right>,
                 TJoinKindTag<EJoinKind::LeftOnly>,
                 TJoinKindTag<EJoinKind::RightOnly>,
                 TJoinKindTag<EJoinKind::LeftSemi>,
                 TJoinKindTag<EJoinKind::RightSemi>,
                 TJoinKindTag<EJoinKind::Full>,
                 TJoinKindTag<EJoinKind::Exclusion>>;

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

constexpr bool ContainsRowsFromInnerJoin(EJoinKind kind) { // true if kind is a join that contains all rows from inner join output.
    switch (kind) {
        using enum EJoinKind;
    case Inner:
    case Full:
    case Left:
    case Right:
    case Cross:
        return true;
    default:
        return false;
    }
}

// Some joins produce concatenation of 2 tuples, some produce one tuple(effectively)
template<typename Fun>
concept JoinMatchFun = std::invocable<Fun, NJoinTable::TTuple> || std::invocable<Fun, NJoinTable::TTuple, NJoinTable::TTuple>;

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


    EFetchResult MatchRows(TComputationContext& ctx, JoinMatchFun auto consumeOneOrTwoTuples) { 
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
                if (Table_.UnusedTrackingOn()) {
                    if constexpr (Kind == EJoinKind::RightSemi) {
                        for (auto& v : Table_.MapView()) {
                            if (v.second.Used) {
                                for (NJoinTable::TTuple used : v.second.Tuples) {
                                    consumeOneOrTwoTuples(used);
                                }
                            }
                        }
                    }
                    Table_.ForEachUnused([&](NJoinTable::TTuple unused) {
                        if constexpr (Kind == EJoinKind::RightOnly) {
                            consumeOneOrTwoTuples(unused);
                        }
                        if constexpr (Kind == EJoinKind::Exclusion || Kind == EJoinKind::Right ||
                                      Kind == EJoinKind::Full) {
                            consumeOneOrTwoTuples(nullptr, unused);
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
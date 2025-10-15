#pragma once
#include "type_utils.h"
#include <util/string/printf.h>
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

namespace NKikimr::NMiniKQL::NJoinTable {

using TTuple = const NYql::NUdf::TUnboxedValue*;
using TSizedTuple = std::span<const NYql::NUdf::TUnboxedValue>;

bool NeedToTrackUnusedRightTuples(EJoinKind kind);

bool NeedToTrackUnusedLeftTuples(EJoinKind kind);

class TStdJoinTable {
    struct TuplesWithSameJoinKey {
        std::vector<TTuple> Tuples;
        bool Used;
    };

  public:
    TStdJoinTable(int tupleSize, NKikimr::NMiniKQL::TWideUnboxedEqual eq, NKikimr::NMiniKQL::TWideUnboxedHasher hash,
                  bool trackUnusedTuples)
        : TupleSize(tupleSize)
        , TrackUnusedTuples(trackUnusedTuples)
        , BuiltTable(1, hash, eq)
    {}

    void Add(TSizedTuple tuple) {
        MKQL_ENSURE(BuiltTable.empty(), "JoinTable is built already");
        MKQL_ENSURE(std::ssize(tuple) == TupleSize, Sprintf("tuple size promise(%i) vs actual(%i) mismatch", TupleSize, std::ssize(tuple)));
        for (int idx = 0; idx < TupleSize; ++idx) {
            Tuples.push_back(tuple[idx]);
        }
    }

    void Build() {
        MKQL_ENSURE(BuiltTable.empty(), "JoinTable is built already");
        for (int index = 0; index < std::ssize(Tuples); index += TupleSize) {
            TTuple thisTuple = &Tuples[index];
            auto [it, ok] = BuiltTable.emplace(
                thisTuple, TuplesWithSameJoinKey{.Tuples = std::vector{thisTuple}, .Used = !TrackUnusedTuples});
            if (!ok) {
                it->second.Tuples.emplace_back(thisTuple);
            }
        }
    }

    void Lookup(TTuple key, std::invocable<TTuple> auto produce) {
        auto it = BuiltTable.find(key);
        if (it != BuiltTable.end()) {
            it->second.Used = true;
            std::ranges::for_each(it->second.Tuples, produce);
        }
    }

    bool UnusedTrackingOn() const {
        return TrackUnusedTuples;
    }

    const auto& MapView() const {
        return BuiltTable;
    }

    void ForEachUnused(std::function<void(TTuple)> produce) {
        MKQL_ENSURE(TrackUnusedTuples, "wasn't tracking tuples at all");
        for (auto& tuplesSameKey : BuiltTable) {
            if (!tuplesSameKey.second.Used) {
                std::ranges::for_each(tuplesSameKey.second.Tuples, produce);
                tuplesSameKey.second.Used = true;
            }
        }
    }

  private:
    const int TupleSize;
    const bool TrackUnusedTuples;
    std::vector<NYql::NUdf::TUnboxedValue> Tuples;
    std::unordered_map<TTuple, TuplesWithSameJoinKey, NKikimr::NMiniKQL::TWideUnboxedHasher,
                       NKikimr::NMiniKQL::TWideUnboxedEqual>
        BuiltTable;
};

} // namespace NKikimr::NMiniKQL::NJoinTable
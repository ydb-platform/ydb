#pragma once
#include "type_utils.h"
#include <util/string/printf.h>
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>
#include <absl/container/flat_hash_map.h>
namespace NKikimr::NMiniKQL::NJoinTable {

using TTuple = const NYql::NUdf::TUnboxedValue*;
using TSizedTuple = std::span<const NYql::NUdf::TUnboxedValue>;
using FlatTuples = std::vector<NYql::NUdf::TUnboxedValue>;

bool NeedToTrackUnusedRightTuples(EJoinKind kind);

bool NeedToTrackUnusedLeftTuples(EJoinKind kind);

class TStdJoinTable {
    struct TuplesWithSameJoinKey {
        FlatTuples Tuples;
        bool Used;
    };



  public:
    TStdJoinTable(int tupleSize, NKikimr::NMiniKQL::TWideUnboxedEqual eq, NKikimr::NMiniKQL::TWideUnboxedHasher hash,
                  bool trackUnusedTuples)
        : TupleSize(tupleSize)
        , TrackUnusedTuples(trackUnusedTuples)
        , BuiltTable( 1,hash, eq)
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
            NYql::NUdf::TUnboxedValue* thisTuple = &Tuples[index];
            FlatTuples flat;
            flat.reserve(TupleSize);
            for(int index = 0; index < TupleSize; ++index) {
                flat.push_back(std::move(thisTuple[index]));
            }
            TuplesWithSameJoinKey newbie{.Tuples = {}, .Used = !TrackUnusedTuples};
            auto [it, _] = BuiltTable.emplace(
                flat, newbie);
            it->second.Tuples.reserve(std::ssize(it->second.Tuples) + TupleSize);
            for(int index = 0; index < TupleSize; ++index) {
                it->second.Tuples.push_back(std::move(flat[index]));
            }
        }
        Tuples.clear();
    }

    void Lookup(TTuple key, std::invocable<TTuple> auto produce) {
        MKQL_ENSURE(Tuples.empty(), "forgot to clear tuples");
        auto it = BuiltTable.find({key, key+TupleSize});
        if (it != BuiltTable.end()) {
            it->second.Used = true;
            ForEachFLat(produce, it->second.Tuples);
        }
    }

    bool UnusedTrackingOn() const {
        return TrackUnusedTuples;
    }

    const auto& MapView() const {
        return BuiltTable;
    }

    void ForEachFLat(std::invocable<TTuple> auto produce, const TSizedTuple flatTuples) const {
        for(int index = 0; index < std::ssize(flatTuples); index += TupleSize){
            produce(&flatTuples[index]);
        }
    }

    void ForEachUnused(std::invocable<TTuple> auto produce) {
        MKQL_ENSURE(TrackUnusedTuples, "wasn't tracking tuples at all");
        for (auto& tuplesSameKey : BuiltTable) {
            if (!tuplesSameKey.second.Used) {
                ForEachFLat(produce, tuplesSameKey.second.Tuples);
                tuplesSameKey.second.Used = true;
            }
        }
    }

  private:
    const int TupleSize;
    const bool TrackUnusedTuples;
    std::deque<NYql::NUdf::TUnboxedValue> Tuples;
    absl::flat_hash_map<FlatTuples, TuplesWithSameJoinKey, NKikimr::NMiniKQL::TWideUnboxedHasher,
                       NKikimr::NMiniKQL::TWideUnboxedEqual>
        BuiltTable;
};

} // namespace NKikimr::NMiniKQL::NJoinTable
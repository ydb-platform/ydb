#pragma once
#include "type_utils.h"
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

namespace NKikimr::NMiniKQL::NJoinTable {

using TTuple = NYql::NUdf::TUnboxedValue*;

class TStdJoinTable {
  public:
    TStdJoinTable(int tupleSize, NKikimr::NMiniKQL::TWideUnboxedEqual eq, NKikimr::NMiniKQL::TWideUnboxedHasher hash)
        : TupleSize(tupleSize), BuiltTable(1, hash, eq)
    {}

    void Add(std::span<NYql::NUdf::TUnboxedValue> tuple) {
        Y_ABORT_UNLESS(BuiltTable.empty(), "JoinTable is built already");
        MKQL_ENSURE(std::ssize(tuple) == TupleSize, "tuple size promise vs actual mismatch");
        for (int idx = 0; idx < TupleSize; ++idx) {
            Tuples.push_back(tuple[idx]);
        }
    }

    void Build() {
        Y_ABORT_UNLESS(BuiltTable.empty(), "JoinTable is built already");
        for (int index = 0; index < std::ssize(Tuples); index += TupleSize) {
            TTuple thisTuple = &Tuples[index];
            auto [it, ok] = BuiltTable.emplace(thisTuple, std::vector{thisTuple});
            if (!ok) {
                it->second.emplace_back(thisTuple);
            }
        }
    }

    void Lookup(TTuple key, std::function<void(TTuple)> produce) const {
        auto it = BuiltTable.find(key);
        if (it != BuiltTable.end()) {
            std::ranges::for_each(it->second, produce);
        }
    }

  private:
    const int TupleSize;
    std::vector<NYql::NUdf::TUnboxedValue> Tuples;
    std::unordered_map<TTuple, std::vector<TTuple>, NKikimr::NMiniKQL::TWideUnboxedHasher,
                       NKikimr::NMiniKQL::TWideUnboxedEqual>
        BuiltTable;
};

} // namespace NKikimr::NMiniKQL::NJoinTable
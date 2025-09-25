#pragma once
#include "type_utils.h"
#include <yql/essentials/minikql/comp_nodes/mkql_rh_hash.h>

namespace NKikimr::NMiniKQL {

using TTuple = std::span<NYql::NUdf::TUnboxedValue>;

}
namespace std{
    template<>
    class hash<NKikimr::NMiniKQL::TTuple>{
        // hash(TKey)
    size_t operator()(NKikimr::NMiniKQL::TTuple vec){
        return Hasher(vec.data());
    }
    NKikimr::NMiniKQL::TWideUnboxedHasher Hasher;
    };
    template<>
    class equal_to<NKikimr::NMiniKQL::TTuple>{
        // hash(TKey)
    bool operator()(NKikimr::NMiniKQL::TTuple lhs, NKikimr::NMiniKQL::TTuple rhs ){
        return Equal(lhs.data(),rhs.data());
    }
    NKikimr::NMiniKQL::TWideUnboxedEqual Equal;
    };
    
}

namespace NKikimr::NMiniKQL {

class TDumbJoinTable{
    void BuildWith(std::vector<std::vector<NYql::NUdf::TUnboxedValue>> buildTable){
        assert(BuiltTable.empty());
        int size = std::ssize(buildTable[0]);

        std::ranges::for_each(buildTable, [&](std::span<NYql::NUdf::TUnboxedValue> tuple) mutable{
            assert(std::ssize(tuple) == size);
            auto [it, ok] = BuiltTable.emplace(tuple, std::vector{tuple});
            if (!ok){
                it->second.emplace_back(tuple);
            }
        });
        Tuples = std::move(buildTable);
    }
void Lookup(std::span<NYql::NUdf::TUnboxedValue> key, std::function<void(const TTuple&)> produce) const {
    auto it = BuiltTable.find(key);
    if (it != BuiltTable.end()){
        std::ranges::for_each(it->second, produce);
    }
}
private:
    std::vector<std::vector<NYql::NUdf::TUnboxedValue>> Tuples;
    std::unordered_map<TTuple,std::vector<TTuple>> BuiltTable;
}

}
#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

#include <algorithm>
#include <vector>

namespace NKikimr {
namespace NMiniKQL {

struct TWideUnboxedEqual {
    TWideUnboxedEqual(const TKeyTypes& types)
        : Types(types)
    {}

    bool operator()(const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right) const {
        for (ui32 i = 0U; i < Types.size(); ++i)
            if (CompareValues(Types[i].first, true, Types[i].second, left[i], right[i]))
                return false;
        return true;
    }

    const TKeyTypes& Types;
};

struct TWideUnboxedHasher {
    TWideUnboxedHasher(const TKeyTypes& types)
        : Types(types)
    {}

    NUdf::THashType operator()(const NUdf::TUnboxedValuePod* values) const {
        if (Types.size() == 1U)
            if (const auto v = *values)
                return NUdf::GetValueHash(Types.front().first, v);
            else
                return HashOfNull;

        NUdf::THashType hash = 0ULL;
        for (const auto& type : Types) {
            if (const auto v = *values++)
                hash = CombineHashes(hash, NUdf::GetValueHash(type.first, v));
            else
                hash = CombineHashes(hash, HashOfNull);
        }
        return hash;
    }

    const TKeyTypes& Types;
};

bool UnwrapBlockTypes(const TArrayRef<TType* const>& typeComponents, std::vector<TType*>& result);

void WrapArrayBlockTypes(std::vector<TType*>& types, const TProgramBuilder& pb);

int ArrowScalarAsInt(const TArrowBlock& scalar);

bool ForceLeftOptional(EJoinKind kind);

// Left join causes all right columns to be nullable
bool ForceRightOptional(EJoinKind kind);

} // namespace NMiniKQL
} // namespace NKikimr
#include "arrow.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <util/generic/singleton.h>

namespace NYql {

extern "C" {
#include "pg_kernels_fwd.inc"
}

struct TExecs {
    static TExecs& Instance() {
        return *Singleton<TExecs>();
    }

    THashMap<Oid, TExecFunc> Table;
};

TExecFunc FindExec(Oid oid) {
    const auto& table = TExecs::Instance().Table;
    auto it = table.find(oid);
    if (it == table.end()) {
        return nullptr;
    }

    return it->second;
}

void RegisterExec(Oid oid, TExecFunc func) {
    auto& table = TExecs::Instance().Table;
    table[oid] = func;
}

bool HasPgKernel(ui32 procOid) {
    return FindExec(procOid) != nullptr;
}

void RegisterPgKernels() {
#include "pg_kernels_register.0.inc"
#include "pg_kernels_register.1.inc"
#include "pg_kernels_register.2.inc"
#include "pg_kernels_register.3.inc"
}

const NPg::TAggregateDesc& ResolveAggregation(const TString& name, NKikimr::NMiniKQL::TTupleType* tupleType, const std::vector<ui32>& argsColumns, NKikimr::NMiniKQL::TType* returnType) {
    using namespace NKikimr::NMiniKQL;
    if (returnType) {
        MKQL_ENSURE(argsColumns.size() == 1, "Expected one column");
        TType* stateType = AS_TYPE(TBlockType, tupleType->GetElementType(argsColumns[0]))->GetItemType();
        TType* returnItemType = AS_TYPE(TBlockType, returnType)->GetItemType();
        return NPg::LookupAggregation(name, AS_TYPE(TPgType, stateType)->GetTypeId(), AS_TYPE(TPgType, returnItemType)->GetTypeId());
    } else {
        TVector<ui32> argTypeIds;
        for (const auto col : argsColumns) {
            argTypeIds.push_back(AS_TYPE(TPgType, AS_TYPE(TBlockType, tupleType->GetElementType(col))->GetItemType())->GetTypeId());
        }

        return NPg::LookupAggregation(name, argTypeIds);
    }
}

}

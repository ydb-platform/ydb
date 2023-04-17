#include "arrow.h"
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
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

}

#include "dq_operator_memory_quota.h"

#include <util/system/tls.h>
#include <util/system/yassert.h>

namespace NYql::NDq {

namespace {

Y_POD_STATIC_THREAD(IDqOperatorMemoryQuota*) TlsOperatorMemoryQuota;

} // namespace

IDqOperatorMemoryQuota* GetDqOperatorMemoryQuota() {
    return TlsOperatorMemoryQuota;
}

void UnbindDqOperatorMemoryQuota(const IDqOperatorMemoryQuota* quota) {
    if (TlsOperatorMemoryQuota == quota) {
        TlsOperatorMemoryQuota = nullptr;
    }
}

TDqOperatorMemoryQuotaScope::TDqOperatorMemoryQuotaScope(IDqOperatorMemoryQuota* quota) {
    // One scope per execution, opened by the owner of the computation graph. A nested scope would have to
    // restore the outer binding on exit, and UnbindDqOperatorMemoryQuota cannot reach a saved value: a quota
    // torn down inside the inner scope would come back as a dangling pointer.
    Y_ABORT_UNLESS(!TlsOperatorMemoryQuota, "operator memory quota scopes must not nest");
    TlsOperatorMemoryQuota = quota;
}

TDqOperatorMemoryQuotaScope::~TDqOperatorMemoryQuotaScope() {
    TlsOperatorMemoryQuota = nullptr;
}

} // namespace NYql::NDq

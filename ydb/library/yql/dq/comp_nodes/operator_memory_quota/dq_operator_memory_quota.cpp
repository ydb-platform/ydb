#include "dq_operator_memory_quota.h"

#include <util/system/tls.h>

namespace NYql::NDq {

namespace {

Y_POD_STATIC_THREAD(IDqOperatorMemoryQuota*) TlsOperatorMemoryQuota;

} // namespace

IDqOperatorMemoryQuota* GetDqOperatorMemoryQuota() {
    return TlsOperatorMemoryQuota;
}

TDqOperatorMemoryQuotaScope::TDqOperatorMemoryQuotaScope(IDqOperatorMemoryQuota* quota)
    : Previous(TlsOperatorMemoryQuota)
{
    TlsOperatorMemoryQuota = quota;
}

TDqOperatorMemoryQuotaScope::~TDqOperatorMemoryQuotaScope() {
    TlsOperatorMemoryQuota = Previous;
}

} // namespace NYql::NDq

#pragma once
#include <ydb/library/services/services.pb.h>

#if defined CA_LOG_D || defined CA_LOG_I || defined CA_LOG_E || defined CA_LOG_C
#   error log macro definition clash
#endif

#define CA_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)
#define CA_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)
#define CA_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)
#define CA_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)
#define CA_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)
#define CA_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)
#define CA_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)
#define CA_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, this->LogPrefix << s)


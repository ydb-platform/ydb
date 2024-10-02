#include "defs.h"
#include "execution_unit_ctors.h"
#include "datashard_active_transaction.h"
#include "datashard_impl.h"

// #define EXPORT_LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)
// #define EXPORT_LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::DATASHARD_BACKUP, "[Export] [" << LogPrefix() << "] " << stream)

namespace NKikimr {
namespace NDataShard {

THolder<TExecutionUnit> CreateIncrementalBackupSrcUnit(TDataShard& self, TPipeline& pipeline) {
    Y_UNUSED(self, pipeline);
    return nullptr;
}

} // NDataShard
} // NKikimr

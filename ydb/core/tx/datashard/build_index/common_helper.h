#pragma once

#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NDataShard {

#define LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_I(stream) LOG_INFO_S  (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_W(stream) LOG_WARN_S  (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_E(stream) LOG_ERROR_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)

inline void StartScan(TDataShard* dataShard, TAutoPtr<NTable::IScan>&& scan, ui64 id, 
    TScanRecord::TSeqNo seqNo, TRowVersion rowVersion, ui32 tableId)
{
    auto& scanManager = dataShard->GetScanManager();

    if (const auto* recCard = scanManager.Get(id)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            return;
        }

        for (auto scanId : recCard->ScanIds) {
            dataShard->CancelScan(tableId, scanId);
        }
        scanManager.Drop(id);
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10);
    const auto scanId = dataShard->QueueScan(tableId, std::move(scan), 0, scanOpts);
    scanManager.Set(id, seqNo).push_back(scanId);
}

}

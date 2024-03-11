#include "tx.h"

#include <ydb/core/base/tx_processing.h>

namespace NKikimr {

TEvTxProxy::TEvProposeTransaction::TEvProposeTransaction(ui64 coordinator, ui64 txId, ui8 execLevel, ui64 minStep, ui64 maxStep) {
    Record.SetCoordinatorID(coordinator);
    NKikimrTx::TProxyTransaction *x = Record.MutableTransaction();
    x->SetTxId(txId);
    x->SetExecLevel(execLevel);
    x->SetMinStep(minStep);
    x->SetMaxStep(maxStep);
}

TEvTxProxy::TEvProposeTransactionStatus::TEvProposeTransactionStatus(TEvTxProxy::TEvProposeTransactionStatus::EStatus status, ui64 txid, ui64 stepId) {
    Record.SetStatus((ui32)status);
    if (txid)
        Record.SetTxId(txid);
    if (stepId)
        Record.SetStepId(stepId);
}

TEvSubDomain::TEvConfigure::TEvConfigure(const NKikimrSubDomains::TProcessingParams &processing) {
    Record.CopyFrom(processing);
}

TEvSubDomain::TEvConfigure::TEvConfigure(NKikimrSubDomains::TProcessingParams &&processing) {
    Record.Swap(&processing);
}

TEvSubDomain::TEvConfigureStatus::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::EStatus status, ui64 tabletId) {
    Record.SetStatus(status);
    Record.SetOnTabletId(tabletId);
}

TAutoPtr<TEvSubDomain::TEvConfigure> CreateDomainConfigurationFromStatic(const TAppData *appdata) {
    return new TEvSubDomain::TEvConfigure(ExtractProcessingParams(*appdata->DomainsInfo->GetDomain()));
}

}

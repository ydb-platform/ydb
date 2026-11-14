#include "tx_processing.h"

namespace NKikimr {

TEvTxProcessing::TEvPlanStep::TEvPlanStep(ui64 step, ui64 mediator, ui64 tablet)
{
    Record.SetStep(step);
    Record.SetMediatorID(mediator);
    Record.SetTabletID(tablet);
}

TString TEvTxProcessing::TEvPlanStep::ToString() const {
    TStringStream str;
    str << "{TEvPlanStep step# " << Record.GetStep();
    str << " MediatorId# " << Record.GetMediatorID();
    str << " TabletID " << Record.GetTabletID();
    str << "}";
    return str.Str();
}

TEvTxProcessing::TEvPlanStepAck::TEvPlanStepAck(ui64 tabletId, ui64 step, ui64 txid)
{
    Record.SetTabletId(tabletId);
    Record.SetStep(step);
    Record.AddTxId(txid);
}

TString TEvTxProcessing::TEvPlanStepAck::ToString() const {
    TStringStream str;
    str << "{TEvPlanStepAck TabletId# " << Record.GetTabletId();
    str << " step# " << Record.GetStep();
    for (size_t i = 0; i < Record.TxIdSize(); ++i) {
        str << " txid# " << Record.GetTxId(i);
    }
    str << "}";
    return str.Str();
}

TEvTxProcessing::TEvPlanStepAccepted::TEvPlanStepAccepted(ui64 tabletId, ui64 step)
{
    Record.SetTabletId(tabletId);
    Record.SetStep(step);
}

TString TEvTxProcessing::TEvPlanStepAccepted::ToString() const {
    TStringStream str;
    str << "{TEvPlanStepAccepted TabletId# " << Record.GetTabletId();
    str << " step# " << Record.GetStep();
    str << "}";
    return str.Str();
}

TEvTxProcessing::TEvReadSet::TEvReadSet(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletProducer)
{
    Record.SetStep(step);
    Record.SetTxId(orderId);
    Record.SetTabletSource(tabletSource);
    Record.SetTabletDest(tabletDest);
    Record.SetTabletProducer(tabletProducer);
}

TEvTxProcessing::TEvReadSet::TEvReadSet(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletProducer, const TString &readSet, ui64 seqno)
{
    Record.SetStep(step);
    Record.SetTxId(orderId);
    Record.SetTabletSource(tabletSource);
    Record.SetTabletDest(tabletDest);
    Record.SetTabletProducer(tabletProducer);
    Record.SetReadSet(readSet);
    Record.SetSeqno(seqno);
}

TString TEvTxProcessing::TEvReadSet::ToString() const {
    TStringStream str;
    str << "{TEvReadSet step# " << Record.GetStep();
    str << " txid# " << Record.GetTxId();
    str << " TabletSource# " << Record.GetTabletSource();
    str << " TabletDest# " << Record.GetTabletDest();
    str << " SetTabletProducer# " << Record.GetTabletProducer();
    str << " ReadSet.Size()# " << Record.GetReadSet().size();
    str << " Seqno# " << Record.GetSeqno();
    str << " Flags# " << Record.GetFlags();
    // BalanceTrackList
    str << "}";
    return str.Str();
}

TEvTxProcessing::TEvReadSetAck::TEvReadSetAck(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletConsumer, ui32 flags, ui64 seqno)
{
    Record.SetStep(step);
    Record.SetTxId(orderId);
    Record.SetTabletSource(tabletSource);
    Record.SetTabletDest(tabletDest);
    Record.SetTabletConsumer(tabletConsumer);
    Record.SetFlags(flags);
    Record.SetSeqno(seqno);
}

TEvTxProcessing::TEvReadSetAck::TEvReadSetAck(const TEvReadSet& evReadSet, ui64 tabletConsumer)
{
    Record.SetStep(evReadSet.Record.GetStep());
    Record.SetTxId(evReadSet.Record.GetTxId());
    Record.SetTabletSource(evReadSet.Record.GetTabletSource());
    Record.SetTabletDest(evReadSet.Record.GetTabletDest());
    Record.SetTabletConsumer(tabletConsumer);
    Record.SetFlags(0);
    Record.SetSeqno(evReadSet.Record.GetSeqno());
}

TString TEvTxProcessing::TEvReadSetAck::ToString() const {
    TStringStream str;
    str << "{TEvReadSet step# " << Record.GetStep();
    str << " txid# " << Record.GetTxId();
    str << " TabletSource# " << Record.GetTabletSource();
    str << " TabletDest# " << Record.GetTabletDest();
    str << " SetTabletConsumer# " << Record.GetTabletConsumer();
    str << " Flags# " << Record.GetFlags();
    str << " Seqno# " << Record.GetSeqno();
    str << "}";
    return str.Str();
}

} // namespace NKikimr

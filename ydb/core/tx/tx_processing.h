#pragma once
#include "defs.h"
#include "tx.h"

namespace NKikimr {

struct TEvTxProcessing {
    enum EEv {
        EvPlanStep = EventSpaceBegin(TKikimrEvents::ES_TX_PROCESSING),
        EvReadSet,
        EvPrePlanTx, // unused
        EvStreamClearanceRequest,
        EvStreamQuotaRequest,
        EvStreamQuotaRelease,
        EvStreamIsDead,
        EvInterruptTransaction,

        EvPlanStepAck = EvPlanStep + 512,
        EvPrePlanTxAck, // unused
        EvReadSetAck,
        EvPlanStepAccepted,
        EvStreamClearanceResponse,
        EvStreamQuotaResponse,
        EvStreamClearancePending,
        EvStreamDataAck,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_PROCESSING), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TX_PROCESSING)");

    struct TEvPlanStep : public TEventPB<TEvPlanStep, NKikimrTx::TEvMediatorPlanStep, EvPlanStep> {
        TEvPlanStep()
        {}

        TEvPlanStep(ui64 step, ui64 mediator, ui64 tablet)
        {
            Record.SetStep(step);
            Record.SetMediatorID(mediator);
            Record.SetTabletID(tablet);
        }

        TString ToString() const {
            TStringStream str;
            str << "{TEvPlanStep step# " << Record.GetStep();
            str << " MediatorId# " << Record.GetMediatorID();
            str << " TabletID " << Record.GetTabletID();
            str << "}";
            return str.Str();
        }
    };

    struct TEvPlanStepAck : public TEventPB<TEvPlanStepAck, NKikimrTx::TEvPlanStepAck, EvPlanStepAck> {
        TEvPlanStepAck()
        {}

        TEvPlanStepAck(ui64 tabletId, ui64 step, ui64 txid)
        {
            Record.SetTabletId(tabletId);
            Record.SetStep(step);
            Record.AddTxId(txid);
        }

        template<typename TIterator>
        TEvPlanStepAck(ui64 tabletId, ui64 step, TIterator begin, const TIterator &end)
        {
            Record.SetTabletId(tabletId);
            Record.SetStep(step);

            while (begin != end) {
                const ui64 x = ui64(*begin);
                Record.AddTxId(x);
                ++begin;
            }
        }

        TString ToString() const {
            TStringStream str;
            str << "{TEvPlanStepAck TabletId# " << Record.GetTabletId();
            str << " step# " << Record.GetStep();
            for (size_t i = 0; i < Record.TxIdSize(); ++i) {
                str << " txid# " << Record.GetTxId(i);
            }
            str << "}";
            return str.Str();
        }
    };

    struct TEvPlanStepAccepted : public TEventPB<TEvPlanStepAccepted, NKikimrTx::TEvPlanStepAccepted, EvPlanStepAccepted> {
        TEvPlanStepAccepted()
        {}

        TEvPlanStepAccepted(ui64 tabletId, ui64 step)
        {
            Record.SetTabletId(tabletId);
            Record.SetStep(step);
        }

        TString ToString() const {
            TStringStream str;
            str << "{TEvPlanStepAccepted TabletId# " << Record.GetTabletId();
            str << " step# " << Record.GetStep();
            str << "}";
            return str.Str();
        }
    };

    struct TEvReadSet: public TEventPB<TEvReadSet, NKikimrTx::TEvReadSet, EvReadSet> {
        TEvReadSet()
        {}

        TEvReadSet(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletProducer)
        {
            Record.SetStep(step);
            Record.SetTxId(orderId);
            Record.SetTabletSource(tabletSource);
            Record.SetTabletDest(tabletDest);
            Record.SetTabletProducer(tabletProducer);
        }

        TEvReadSet(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletProducer, const TString &readSet, ui64 seqno = 0)
        {
            Record.SetStep(step);
            Record.SetTxId(orderId);
            Record.SetTabletSource(tabletSource);
            Record.SetTabletDest(tabletDest);
            Record.SetTabletProducer(tabletProducer);
            Record.SetReadSet(readSet);
            Record.SetSeqno(seqno);
        }

        TString ToString() const {
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
    };

    struct TEvReadSetAck : public TThrRefBase, public TEventPB<TEvReadSetAck, NKikimrTx::TEvReadSetAck, EvReadSetAck> {
        TEvReadSetAck()
        {}

        TEvReadSetAck(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletConsumer, ui32 flags, ui64 seqno = 0)
        {
            Record.SetStep(step);
            Record.SetTxId(orderId);
            Record.SetTabletSource(tabletSource);
            Record.SetTabletDest(tabletDest);
            Record.SetTabletConsumer(tabletConsumer);
            Record.SetFlags(flags);
            Record.SetSeqno(seqno);
        }

        TEvReadSetAck(const TEvReadSet& evReadSet, ui64 tabletConsumer)
        {
            Record.SetStep(evReadSet.Record.GetStep());
            Record.SetTxId(evReadSet.Record.GetTxId());
            Record.SetTabletSource(evReadSet.Record.GetTabletSource());
            Record.SetTabletDest(evReadSet.Record.GetTabletDest());
            Record.SetTabletConsumer(tabletConsumer);
            Record.SetFlags(0);
            Record.SetSeqno(evReadSet.Record.GetSeqno());
        }

        TString ToString() const {
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
    };

    struct TEvStreamClearanceRequest: public TEventPB<TEvStreamClearanceRequest,
                                                      NKikimrTx::TEvStreamClearanceRequest,
                                                      EvStreamClearanceRequest>
    {
    };

    struct TEvStreamClearanceResponse: public TEventPB<TEvStreamClearanceResponse,
                                                       NKikimrTx::TEvStreamClearanceResponse,
                                                       EvStreamClearanceResponse>
    {
    };

    struct TEvStreamClearancePending: public TEventPB<TEvStreamClearancePending,
                                                      NKikimrTx::TEvStreamClearancePending,
                                                      EvStreamClearancePending>
    {
        TEvStreamClearancePending() = default;

        TEvStreamClearancePending(ui64 txId)
        {
            Record.SetTxId(txId);
        }
    };

    struct TEvStreamQuotaRequest: public TEventPB<TEvStreamQuotaRequest,
                                                  NKikimrTx::TEvStreamQuotaRequest,
                                                  EvStreamQuotaRequest>
    {
    };

    struct TEvStreamQuotaResponse: public TEventPB<TEvStreamQuotaResponse,
                                                   NKikimrTx::TEvStreamQuotaResponse,
                                                   EvStreamQuotaResponse>
    {
    };

    struct TEvStreamQuotaRelease: public TEventPB<TEvStreamQuotaRelease,
                                                  NKikimrTx::TEvStreamQuotaRelease,
                                                  EvStreamQuotaRelease>
    {
    };

    struct TEvStreamIsDead: public TEventPB<TEvStreamIsDead,
                                            NKikimrTx::TEvStreamIsDead,
                                            EvStreamIsDead>
    {
        TEvStreamIsDead(ui64 txId = 0)
        {
            Record.SetTxId(txId);
        }
    };

    struct TEvInterruptTransaction: public TEventPB<TEvInterruptTransaction,
                                                    NKikimrTx::TEvInterruptTransaction,
                                                    EvInterruptTransaction>
    {
        TEvInterruptTransaction() = default;

        TEvInterruptTransaction(ui64 txId)
        {
            Record.SetTxId(txId);
        }
    };

    struct TEvStreamDataAck: public TEventPB<TEvStreamDataAck,
                                             NKikimrTx::TEvStreamDataAck,
                                             EvStreamDataAck>
    {
    };
};

}

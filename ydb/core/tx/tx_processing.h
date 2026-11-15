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

        TEvPlanStep(ui64 step, ui64 mediator, ui64 tablet);

        TString ToString() const;
    };

    struct TEvPlanStepAck : public TEventPB<TEvPlanStepAck, NKikimrTx::TEvPlanStepAck, EvPlanStepAck> {
        TEvPlanStepAck()
        {}

        TEvPlanStepAck(ui64 tabletId, ui64 step, ui64 txid);

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

        TString ToString() const;
    };

    struct TEvPlanStepAccepted : public TEventPB<TEvPlanStepAccepted, NKikimrTx::TEvPlanStepAccepted, EvPlanStepAccepted> {
        TEvPlanStepAccepted()
        {}

        TEvPlanStepAccepted(ui64 tabletId, ui64 step);

        TString ToString() const;
    };

    struct TEvReadSet: public TEventPB<TEvReadSet, NKikimrTx::TEvReadSet, EvReadSet> {
        TEvReadSet()
        {}

        TEvReadSet(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletProducer);

        TEvReadSet(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletProducer, const TString &readSet, ui64 seqno = 0);

        TString ToString() const;
    };

    struct TEvReadSetAck : public TThrRefBase, public TEventPB<TEvReadSetAck, NKikimrTx::TEvReadSetAck, EvReadSetAck> {
        TEvReadSetAck()
        {}

        TEvReadSetAck(ui64 step, ui64 orderId, ui64 tabletSource, ui64 tabletDest, ui64 tabletConsumer, ui32 flags, ui64 seqno = 0);

        TEvReadSetAck(const TEvReadSet& evReadSet, ui64 tabletConsumer);

        TString ToString() const;
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

#include "pq_impl.h"

namespace NKikimr::NPQ {

TString MakeReadSetData(bool commit)
{
    NKikimrTx::TReadSetData data;
    data.SetDecision(commit ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT);

    TString encoded;
    Y_ABORT_UNLESS(data.SerializeToString(&encoded));

    return encoded;
}

template <class T>
TMaybe<T> GetParameter(NMon::TEvRemoteHttpInfo::TPtr& ev, const TString& name)
{
    if (!ev->Get()->Cgi().Has(name)) {
        return Nothing();
    }
    return FromString<T>(ev->Get()->Cgi().Get(name));
}

bool TPersQueue::OnSendReadSetToYourself(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx)
{
    auto step = GetParameter<ui64>(ev, "step");
    auto txId = GetParameter<ui64>(ev, "txId");
    auto senderTablet = GetParameter<ui64>(ev, "senderTablet");
    auto decision = GetParameter<TString>(ev, "decision");

    if (!step.Defined() || !txId.Defined() || !senderTablet.Defined() || !decision.Defined()) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"expected step, txId, senderTablet and decision"})"));
        return true;
    }

    auto* tx = GetTransaction(ctx, *txId);
    if (!tx) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"unknown tx"})"));
        return true;
    }
    if (tx->Step != *step) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"invalid step"})"));
        return true;
    }

    auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(*step,
                                                               *txId,
                                                               *senderTablet,
                                                               TabletID(),
                                                               *senderTablet,
                                                               MakeReadSetData(*decision == "commit"),
                                                               0);
    ctx.Send(ctx.SelfID, std::move(event));

    ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"OK"})"));

    return true;
}

}

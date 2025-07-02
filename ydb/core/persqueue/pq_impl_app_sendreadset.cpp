#include "pq_impl.h"

#include "common_app.h"
#include <fmt/format.h>

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

TString TPersQueue::RenderSendReadSetHtmlForms(const TDistributedTransaction& tx, ui64 tabletSource) const
{
    struct TOption {
        const char* Decision;
        const char* Text;
        const char* BtnClass;
    };
    static constexpr TOption options[] = {
        {"commit", "Send commit to {txId}", "btn btn-warning btn-sm"},
        {"abort", "Send abort to {txId}", "btn btn-danger btn-sm"},
    };

    TStringStream str;
    HTML(str) {
        for (const TOption& option : options) {
            LAYOUT_COLUMN() {
                FORM_CLASS("form-horizontal") {
                    DIV_CLASS("control-group") {
                        DIV_CLASS("controls") {
                            str << "<input type='hidden' name='step' value='" << tx.Step << "'/>";
                            str << "<input type='hidden' name='txId' value='" << tx.TxId << "'/>";
                            str << "<input type='hidden' name='senderTablet' value='" << tabletSource << "'/>";
                            str << "<input type='hidden' name='decision' value='" << option.Decision << "'/>";
                            str << "<input type='hidden' name='TabletID' value='" << TabletID() << "'/>";
                            str << "<button type='submit' name='SendReadSet' class='" << option.BtnClass << "'>" << fmt::format(fmt::runtime(option.Text), fmt::arg("txId", tx.TxId)) << "</button>";
                        }
                    }
                }
            }
        }
    }
    return std::move(str).Str();
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

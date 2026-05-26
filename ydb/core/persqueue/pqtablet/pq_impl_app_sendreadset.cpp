#include "pq_impl.h"

#include <ydb/core/base/auth.h>
#include <ydb/core/base/mon_auth.h>
#include <ydb/core/persqueue/common/common_app.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/monlib/service/pages/mon_page.h>
#include <fmt/format.h>

namespace NKikimr::NPQ {

namespace {

TString BuildSendReadSetFormAction(TStringBuf pathInfo, ui64 tabletId) {
    if (IsTabletDevUiSecurePath(pathInfo)) {
        return "?";
    }
    return TStringBuilder() << TABLET_DEV_UI_SECURE_MON_RELATIVE_PATH << "?TabletID=" << tabletId;
}

bool IsSendReadSetDevUiAllowed(const TAppData* appData, TStringBuf pathInfo, const TString& userToken) {
    return IsTabletDevUiSecurePath(pathInfo) && IsAdministrator(appData, userToken);
}

} // namespace

static TString MakeReadSetData(bool commit)
{
    NKikimrTx::TReadSetData data;
    data.SetDecision(commit ? NKikimrTx::TReadSetData::DECISION_COMMIT : NKikimrTx::TReadSetData::DECISION_ABORT);

    TString encoded;
    AFL_ENSURE(data.SerializeToString(&encoded));

    return encoded;
}

template <class T>
static TMaybe<T> GetParameter(const TCgiParameters& cgi, const TStringBuf name)
{
    if (!cgi.Has(name)) {
        return Nothing();
    }
    return FromString<T>(cgi.Get(name));
}

template <class T>
static TVector<T> GetParameters(const TCgiParameters& cgi, const TStringBuf name)
{
    const auto [first, last] = cgi.equal_range(name);
    TVector<T> result;
    for (auto it = first; it != last; ++it) {
        result.push_back(FromString<T>(it->second));
    }
    return result;
}


TString TPersQueue::RenderSendReadSetHtmlForms(
    const TDistributedTransaction& tx,
    const TMaybe<TConstArrayRef<ui64>> tabletSourcesFilter,
    TStringBuf pathInfo) const
{
    const TString formAction = BuildSendReadSetFormAction(pathInfo, TabletID());
    struct TOption {
        const char* Decision;
        const char* Text;
        const char* BtnClass;
    };
    static constexpr TOption options[] = {
        {"commit", "Send \"COMMIT\" to {txId}", "btn btn-warning btn-sm"},
        {"abort", "Send \"ABORT\" to {txId}", "btn btn-danger btn-sm"},
    };

    TStringStream str;
    HTML(str) {
        LAYOUT_ROW() {
            for (const TOption& option : options) {
                LAYOUT_COLUMN() {
                    str << "<form class=\"form-horizontal\" action=\"" << formAction << "\" method=\"get\">";
                    {
                        DIV_CLASS("control-group") {
                            DIV_CLASS("controls") {
                                if (tabletSourcesFilter.Defined()) {
                                    for (const ui64 tabletSource : *tabletSourcesFilter) {
                                        str << "<input type='hidden' name='senderTablet' value='" << tabletSource << "'/>";
                                    }
                                } else {
                                    str << "<input type='hidden' name='allSenderTablets' value='1'/>";
                                }
                                str << "<input type='hidden' name='TabletID' value='" << TabletID() << "'/>";
                                str << "<input type='hidden' name='step' value='" << tx.Step << "'/>";
                                str << "<input type='hidden' name='txId' value='" << tx.TxId << "'/>";
                                str << "<input type='hidden' name='decision' value='" << option.Decision << "'/>";
                                str << "<button type='submit' name='SendReadSet' class='" << option.BtnClass << "'>" << fmt::format(fmt::runtime(option.Text), fmt::arg("txId", tx.TxId)) << "</button>";
                            }
                        }
                    }
                    str << "</form>";
                }
            }
        }
    }
    return std::move(str).Str();
}

static TVector<ui64> GetSenderTablets(const TCgiParameters& cgi, const TDistributedTransaction& tx)
{
    if (cgi.Has("allSenderTablets")) {
        TVector<ui64> senderTablets;
        for (const auto& [tabletID, predicate] : tx.PredicatesReceived) {
            if (!predicate.HasPredicate()) {
                senderTablets.push_back(tabletID);
            }
        }
        return senderTablets;
    } else {
        return GetParameters<ui64>(cgi, "senderTablet");
    }
}


bool TPersQueue::OnSendReadSetToYourself(NMon::TEvRemoteHttpInfo::TPtr& ev, const TActorContext& ctx)
{
    if (!IsSendReadSetDevUiAllowed(AppData(ctx), ev->Get()->PathInfo(), ev->Get()->GetUserToken())) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteBinaryInfoRes(NMonitoring::HTTPFORBIDDEN));
        return true;
    }

    const TCgiParameters& cgi = ev->Get()->Cgi();
    const auto step = GetParameter<ui64>(cgi, "step");
    const auto txId = GetParameter<ui64>(cgi, "txId");
    const auto decision = GetParameter<TString>(cgi, "decision");
    const bool hasAllSenderTablets = cgi.Has("allSenderTablets");
    const bool hasSenderTablet = cgi.Has("senderTablet");

    if (!step.Defined() || !txId.Defined() || !decision.Defined()) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"expected step, txId and decision"})"));
        return true;
    }
    if (!hasAllSenderTablets && !hasSenderTablet) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"expected senderTablet parameter or allSenderTablets flag"})"));
        return true;
    } else if (hasAllSenderTablets && hasSenderTablet) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"senderTablet parameter and allSenderTablets flag are mutually exclusive"})"));
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
    const auto senderTablets = GetSenderTablets(cgi, *tx);
    if (senderTablets.empty()) {
        ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"empty senderTablets array"})"));
        return true;
    }

    for (const auto senderTablet : senderTablets) {
        auto event = std::make_unique<TEvTxProcessing::TEvReadSet>(*step,
                                                                   *txId,
                                                                   senderTablet,
                                                                   TabletID(),
                                                                   senderTablet,
                                                                   MakeReadSetData(decision == "commit"),
                                                                   0);
        ctx.Send(ctx.SelfID, std::move(event));
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteJsonInfoRes(R"({"result":"OK"})"));

    return true;
}

}

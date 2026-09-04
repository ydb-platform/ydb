#include "immediate_control_board_actor.h"

#include <ydb/core/control/lib/immediate_control_board_html_renderer.h>

#include <ydb/core/mon/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/services/services.pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

using namespace NActors;

namespace {

const TDuration CountersUpdateInterval = TDuration::Seconds(1);

}

class TImmediateControlActor : public TActorBootstrapped<TImmediateControlActor> {
    struct TLogRecord {
        TInstant Timestamp;
        TString ParamName;
        TAtomicBase PrevValue;
        TAtomicBase NewValue;

        TLogRecord(TInstant timestamp, TString paramName, TAtomicBase prevValue, TAtomicBase newValue)
            : Timestamp(timestamp)
            , ParamName(paramName)
            , PrevValue(prevValue)
            , NewValue(newValue)
        {}

        TString TimestampToStr() {
            struct tm t_p;
            Timestamp.LocalTime(&t_p);
            return Sprintf("%4d-%02d-%02d %02d:%02d:%02d", (int)t_p.tm_year + 1900, (int)t_p.tm_mon + 1,
                    (int)t_p.tm_mday, (int)t_p.tm_hour, (int)t_p.tm_min, (int)t_p.tm_sec);
        }
    };

    TIntrusivePtr<TControlBoard> Icb;
    TIntrusivePtr<TDynamicControlBoard> Dcb;
    TVector<TLogRecord> HistoryLog;

    ::NMonitoring::TDynamicCounters::TCounterPtr HasChanged;
    ::NMonitoring::TDynamicCounters::TCounterPtr ChangedCount;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMMEDIATE_CONTROL_BOARD;
    }

    TImmediateControlActor(
                            TIntrusivePtr<TControlBoard> board,
                            TIntrusivePtr<TDynamicControlBoard> dcb,
                            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Icb(board)
        , Dcb(dcb)
    {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> IcbGroup = GetServiceCounters(counters, "utils");
        HasChanged = IcbGroup->GetCounter("Icb/HasChangedContol");
        ChangedCount = IcbGroup->GetCounter("Icb/ChangedControlsCount");
    }


    void Bootstrap(const TActorContext &ctx) {
        UpdateChangedCounters();
        auto mon = AppData(ctx)->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "icb", "Immediate Control Board", false,
                    ctx.ActorSystem(), ctx.SelfID);
        }
        Become(&TThis::StateFunc);
        ctx.Schedule(CountersUpdateInterval, new TEvents::TEvWakeup());
    }

private:
    void UpdateChangedCounters() {
        const ui64 count = Icb->GetOverriddenCount() + Dcb->GetOverriddenCount();
        *ChangedCount = count;
        *HasChanged = count > 0;
    }

    void HandlePostParams(const TCgiParameters &cgi) {
        if (cgi.Get("__icb_action") == "resetOverride") {
            const TString& controlName = cgi.Get("__icb_control");
            const TString& controlBoard = cgi.Get("__icb_board");
            TControlMutation mutation;
            bool controlExists = false;
            if (controlBoard == "static") {
                if (auto control = Icb->GetControlByName(controlName)) {
                    mutation = control->ClearOverride();
                    controlExists = true;
                }
            } else if (controlBoard == "dynamic") {
                controlExists = Dcb->ClearOverride(controlName, mutation);
            }
            if (controlExists && (
                    mutation.Before.Value != mutation.After.Value ||
                    mutation.Before.Default != mutation.After.Default ||
                    mutation.Before.Overridden != mutation.After.Overridden))
            {
                HistoryLog.emplace_back(
                    TInstant::Now(),
                    controlName,
                    mutation.Before.Value,
                    mutation.After.Value);
            }
            return;
        }
        if (cgi.Has("restoreDefaults")) {
            Icb->RestoreDefaults();
            Dcb->RestoreDefaults();
            HistoryLog.emplace_back(TInstant::Now(), "RestoreDefaults", 0, 0);
        }
        for (const auto& [paramName, paramValue] : cgi) {
            if (paramName == "restoreDefaults") {
                continue;
            }
            TControlMutation mutation;
            bool controlExists = false;
            const TAtomicBase newValue = strtoull(paramValue.data(), nullptr, 10);
            if (auto control = Icb->GetControlByName(paramName)) {
                mutation = control->SetFromHtmlRequestWithState(newValue);
                controlExists = true;
            } else {
                controlExists = Dcb->SetValue(paramName, newValue, mutation);
            }
            if (controlExists && (
                    mutation.Before.Value != mutation.After.Value ||
                    mutation.Before.Default != mutation.After.Default ||
                    mutation.Before.Overridden != mutation.After.Overridden))
            {
                HistoryLog.emplace_back(
                    TInstant::Now(),
                    paramName,
                    mutation.Before.Value,
                    mutation.After.Value);
            }
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        HTTP_METHOD method = ev->Get()->Request.GetMethod();
        if (method == HTTP_METHOD_POST) {
            HandlePostParams(ev->Get()->Request.GetPostParams());
        }
        UpdateChangedCounters();
        TStringStream str;

        TControlBoardTableHtmlRenderer renderer;
        renderer.AddNewTable("Static Controls", EControlBoardType::Static);
        Icb->RenderAsHtml(renderer);
        renderer.AddNewTable("Dynamic Controls", EControlBoardType::Dynamic);
        Dcb->RenderAsHtml(renderer);

        str << renderer.GetHtml();
        HTML(str) {
            str << "<h3>History</h3>";
            TABLE_SORTABLE_CLASS("historyLogTable") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() {str << "Timestamp"; }
                        TABLEH() {str << "Parameter"; }
                        TABLEH() {str << "PrevValue"; }
                        TABLEH() {str << "NewValue"; }
                    }
                }
                TABLEBODY() {
                    for (auto &record : HistoryLog) {
                        TABLER() {
                            TABLED() { str << record.TimestampToStr(); }
                            TABLED() { str << record.ParamName; }
                            TABLED() { str << record.PrevValue; }
                            TABLED() { str << record.NewValue; }
                        }
                    }
                }
            }
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    void Handle(TEvents::TEvWakeup::TPtr&, const TActorContext& ctx) {
        UpdateChangedCounters();
        ctx.Schedule(CountersUpdateInterval, new TEvents::TEvWakeup());
    }

    STFUNC(StateFunc) {
        switch(ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
        }
    }
};

NActors::IActor* CreateImmediateControlActor(
                    TIntrusivePtr<TControlBoard> icb,
                    TIntrusivePtr<TDynamicControlBoard> dcb,
                     const TIntrusivePtr<::NMonitoring::TDynamicCounters> &counters) {
    return new NKikimr::TImmediateControlActor(icb, dcb, counters);
}

};

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

class TImmediateControlActor : public TActorBootstrapped<TImmediateControlActor> {
    struct TLogRecord {
        TInstant Timestamp;
        TString ParamName;
        TAtomicBase PrevValue;
        TAtomicBase NewValue;
        // Operator action, including transitions that keep the same numeric value.
        TString Action;

        TLogRecord(TInstant timestamp, TString paramName, TAtomicBase prevValue, TAtomicBase newValue, TString action)
            : Timestamp(timestamp)
            , ParamName(paramName)
            , PrevValue(prevValue)
            , NewValue(newValue)
            , Action(action)
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
        auto mon = AppData(ctx)->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "icb", "Immediate Control Board", false,
                    ctx.ActorSystem(), ctx.SelfID);
        }
        Become(&TThis::StateFunc);
    }

private:
    // Record numeric changes and override transitions from the same mutation.
    void RecordChange(const TString& name, const TControlMutation& mutation) {
        if (mutation.Before.Value == mutation.After.Value &&
                mutation.Before.Overridden == mutation.After.Overridden)
        {
            return;
        }
        HistoryLog.emplace_back(
            TInstant::Now(),
            name,
            mutation.Before.Value,
            mutation.After.Value,
            mutation.After.Overridden ? "Set override" : "Restore default");
    }

    void HandlePostParams(const TCgiParameters &cgi) {
        if (cgi.Has("restoreDefault")) {
            const TString& controlName = cgi.Get("restoreDefault");
            TControlMutation mutation;
            bool controlExists = false;
            if (auto control = Icb->GetControlByName(controlName)) {
                mutation = control->RestoreDefault();
                controlExists = true;
            } else {
                controlExists = Dcb->RestoreDefault(controlName, mutation);
            }
            if (controlExists) {
                RecordChange(controlName, mutation);
            }
            return;
        }
        if (cgi.Has("restoreDefaults")) {
            Icb->RestoreDefaults();
            Dcb->RestoreDefaults();
            HistoryLog.emplace_back(TInstant::Now(), "RestoreDefaults", 0, 0, "Restore defaults");
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
            if (controlExists) {
                RecordChange(paramName, mutation);
            }
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        HTTP_METHOD method = ev->Get()->Request.GetMethod();
        if (method == HTTP_METHOD_POST) {
            HandlePostParams(ev->Get()->Request.GetPostParams());
        }
        TStringStream str;

        TControlBoardTableHtmlRenderer renderer;
        renderer.AddNewTable("Static Controls");
        Icb->RenderAsHtml(renderer);
        renderer.AddNewTable("Dynamic Controls");
        Dcb->RenderAsHtml(renderer);

        const ui64 count = renderer.GetOverriddenCount();
        *ChangedCount = count;
        *HasChanged = count > 0;

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
                        TABLEH() {str << "Action"; }
                    }
                }
                TABLEBODY() {
                    for (auto &record : HistoryLog) {
                        TABLER() {
                            TABLED() { str << record.TimestampToStr(); }
                            TABLED() { str << record.ParamName; }
                            TABLED() { str << record.PrevValue; }
                            TABLED() { str << record.NewValue; }
                            TABLED() { str << record.Action; }
                        }
                    }
                }
            }
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    STFUNC(StateFunc) {
        switch(ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, Handle);
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

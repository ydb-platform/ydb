#include "immediate_control_board_actor.h"

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

    TIntrusivePtr<TControlBoard> Board;
    TVector<TLogRecord> HistoryLog;

    ::NMonitoring::TDynamicCounters::TCounterPtr HasChanged;
    ::NMonitoring::TDynamicCounters::TCounterPtr ChangedCount;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::IMMEDIATE_CONTROL_BOARD;
    }

    TImmediateControlActor(TIntrusivePtr<TControlBoard> board,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Board(board)
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
                    ctx.ExecutorThread.ActorSystem, ctx.SelfID);
        }
        Become(&TThis::StateFunc);
    }

private:
    void HandlePostParams(const TCgiParameters &cgi) {
        if (cgi.Has("restoreDefaults")) {
            Board->RestoreDefaults();
            HistoryLog.emplace_back(TInstant::Now(), "RestoreDefaults", 0, 0);
            *HasChanged = 0;
            *ChangedCount = 0;
        }
        for (const auto &param : cgi) {
            TAtomicBase newValue = strtoull(param.second.data(), nullptr, 10);
            TAtomicBase prevValue = newValue;
            bool isDefault = Board->SetValue(param.first, newValue, prevValue);
            if (prevValue != newValue) {
                HistoryLog.emplace_back(TInstant::Now(), param.first, prevValue, newValue);
                if (isDefault) {
                    ChangedCount->Dec();
                } else {
                    ChangedCount->Inc();
                }
                *HasChanged = (ui64)ChangedCount->Val() > 0;
            }
        }
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        HTTP_METHOD method = ev->Get()->Request.GetMethod();
        if (method == HTTP_METHOD_POST) {
            HandlePostParams(ev->Get()->Request.GetPostParams());
        }
        TStringStream str;
        str << Board->RenderAsHtml();
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

    STFUNC(StateFunc) {
        switch(ev->GetTypeRewrite()) {
            HFunc(NMon::TEvHttpInfo, Handle);
        }
    }
};

NActors::IActor* CreateImmediateControlActor(TIntrusivePtr<TControlBoard> board,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new NKikimr::TImmediateControlActor(board, counters);
}
};

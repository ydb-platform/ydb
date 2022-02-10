#include "tablet_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxConfigSet : public TTxBase {
    const TActorId Sender;
    const ui64 Cookie;
    const NKikimrKesus::TEvSetConfig Record;

    THolder<TEvKesus::TEvSetConfigResult> Reply;

    TTxConfigSet(TSelf* self, const TActorId& sender, ui64 cookie, const NKikimrKesus::TEvSetConfig& record)
        : TTxBase(self)
        , Sender(sender)
        , Cookie(cookie)
        , Record(record)
    {}

    TTxType GetTxType() const override { return TXTYPE_CONFIG_SET; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxConfigSet::Execute (sender=" << Sender
                << ", cookie=" << Cookie << ", path=" << Record.GetConfig().path().Quote() << ")");

        Reply = MakeHolder<TEvKesus::TEvSetConfigResult>(Record.GetTxId(), Self->TabletID());

        ui64 newVersion = Record.GetVersion();
        if (newVersion == 0) {
            newVersion = 1; // old schemeshard, assume version 1
        }
        if (newVersion < Self->ConfigVersion) {
            Reply->SetError(Ydb::StatusIds::PRECONDITION_FAILED, "Config downgrade not allowed");
            return true;
        }

        TString newPath = Record.GetPath();
        if (newPath.empty()) {
            newPath = Record.GetConfig().path(); // TODO: remove legacy field eventually
        }

        NIceDb::TNiceDb db(txc.DB);

        if (newPath) {
            if (Self->KesusPath != newPath) {
                Self->QuoterResources.SetQuoterCounters(GetServiceCounters(AppData()->Counters, "quoter_service")->GetSubgroup("quoter", newPath));
                Self->QuoterResources.SetQuoterPath(newPath);
            }
            Self->KesusPath = newPath;
            Self->PersistSysParam(db, Schema::SysParam_KesusPath, Self->KesusPath);
        }

        if (Record.GetConfig().self_check_period_millis()) {
            Self->SelfCheckPeriod = TDuration::MilliSeconds(Record.GetConfig().self_check_period_millis());
            Self->PersistSysParam(db, Schema::SysParam_SelfCheckPeriodMillis, ToString(Self->SelfCheckPeriod.MilliSeconds()));
        }

        if (Record.GetConfig().session_grace_period_millis()) {
            Self->SessionGracePeriod = TDuration::MilliSeconds(Record.GetConfig().session_grace_period_millis());
            Self->PersistSysParam(db, Schema::SysParam_SessionGracePeriodMillis, ToString(Self->SessionGracePeriod.MilliSeconds()));
        }

        if (Record.GetConfig().read_consistency_mode()) {
            Self->ReadConsistencyMode = Record.GetConfig().read_consistency_mode();
            Self->PersistSysParam(db, Schema::SysParam_ReadConsistencyMode, ToString(static_cast<ui64>(Self->ReadConsistencyMode)));
        }

        if (Record.GetConfig().attach_consistency_mode()) {
            Self->AttachConsistencyMode = Record.GetConfig().attach_consistency_mode();
            Self->PersistSysParam(db, Schema::SysParam_AttachConsistencyMode, ToString(static_cast<ui64>(Self->AttachConsistencyMode)));
        }

        if (Record.GetConfig().rate_limiter_counters_mode()) {
            Self->RateLimiterCountersMode = Record.GetConfig().rate_limiter_counters_mode();
            Self->PersistSysParam(db, Schema::SysParam_RateLimiterCountersMode, ToString(static_cast<ui64>(Self->RateLimiterCountersMode)));

            // Apply mode to resource tree
            Self->QuoterResources.EnableDetailedCountersMode(Self->RateLimiterCountersMode == Ydb::Coordination::RATE_LIMITER_COUNTERS_MODE_DETAILED);
        }

        Self->ConfigVersion = newVersion;
        Self->PersistSysParam(db, Schema::SysParam_ConfigVersion, ToString(Self->ConfigVersion));

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_TABLET,
            "[" << Self->TabletID() << "] TTxConfigSet::Complete (sender=" << Sender
                << ", cookie=" << Cookie << ", status=" << Reply->Record.GetError().GetStatus() << ")");

        ctx.Send(Sender, Reply.Release(), 0, Cookie);
    }
};

void TKesusTablet::Handle(TEvKesus::TEvSetConfig::TPtr& ev) {
    Execute(new TTxConfigSet(this, ev->Sender, ev->Cookie, ev->Get()->Record), TActivationContext::AsActorContext());
}

}
}

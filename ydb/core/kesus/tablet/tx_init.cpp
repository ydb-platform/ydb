#include "tablet_impl.h"

#include "quoter_resource_tree.h"
#include "schema.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/appdata.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NKesus {

struct TKesusTablet::TTxInit : public TTxBase {
    TActorId PreviousTabletActorID;

    explicit TTxInit(TSelf* self)
        : TTxBase(self)
    {}

    TTxType GetTxType() const override { return TXTYPE_INIT; }

    void Reset() {
        PreviousTabletActorID = {};
        Self->ResetState();
        Self->ResetCounters();
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::KESUS_TABLET, "[%lu] TTxInit::Execute", Self->TabletID());

        Reset();

        NIceDb::TNiceDb db(txc.DB);

        {
            // precharge everything
            auto sysParamsRowset = db.Table<Schema::SysParams>().Range().Select();
            auto sessionsRowset = db.Table<Schema::Sessions>().Range().Select();
            auto semaphoresRowset = db.Table<Schema::Semaphores>().Range().Select();
            auto sessionSemaphoresRowset = db.Table<Schema::SessionSemaphores>().Range().Select();
            auto quoterResourcesRowset = db.Table<Schema::QuoterResources>().Range().Select();
            if (!sysParamsRowset.IsReady() ||
                !sessionsRowset.IsReady() ||
                !semaphoresRowset.IsReady() ||
                !sessionSemaphoresRowset.IsReady() ||
                !quoterResourcesRowset.IsReady())
            {
                return false;
            }
        }

        // Read SysParams
        {
            auto sysParamsRowset = db.Table<Schema::SysParams>().Range().Select();
            if (!sysParamsRowset.IsReady())
                return false;
            while (!sysParamsRowset.EndOfSet()) {
                ui64 id = sysParamsRowset.GetValue<Schema::SysParams::Id>();
                TString value = sysParamsRowset.GetValue<Schema::SysParams::Value>();
                switch (id) {
                    case Schema::SysParam_KesusPath:
                        Self->KesusPath = value;
                        break;
                    case Schema::SysParam_NextSessionId:
                        Self->NextSessionId = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_NextSemaphoreId:
                        Self->NextSemaphoreId = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_NextSemaphoreOrderId:
                        Self->NextSemaphoreOrderId = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_LastLeaderActor:
                        Y_ABORT_UNLESS(PreviousTabletActorID.Parse(value.data(), value.size()));
                        break;
                    case Schema::SysParam_SelfCheckPeriodMillis:
                        if (auto millis = FromString<ui64>(value)) {
                            Self->SelfCheckPeriod = TDuration::MilliSeconds(millis);
                        }
                        break;
                    case Schema::SysParam_SessionGracePeriodMillis:
                        if (auto millis = FromString<ui64>(value)) {
                            Self->SessionGracePeriod = TDuration::MilliSeconds(millis);
                        }
                        break;
                    case Schema::SysParam_SelfCheckCounter:
                        Self->SelfCheckCounter = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_ConfigVersion:
                        Self->ConfigVersion = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_ReadConsistencyMode:
                        if (auto mode = FromString<ui64>(value)) {
                            Self->ReadConsistencyMode = static_cast<Ydb::Coordination::ConsistencyMode>(mode);
                        }
                        break;
                    case Schema::SysParam_AttachConsistencyMode:
                        if (auto mode = FromString<ui64>(value)) {
                            Self->AttachConsistencyMode = static_cast<Ydb::Coordination::ConsistencyMode>(mode);
                        }
                        break;
                    case Schema::SysParam_StrictMarkerCounter:
                        Self->StrictMarkerCounter = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_NextQuoterResourceId:
                        Self->NextQuoterResourceId = FromString<ui64>(value);
                        break;
                    case Schema::SysParam_RateLimiterCountersMode:
                        if (auto mode = FromString<ui64>(value)) {
                            Self->RateLimiterCountersMode = static_cast<Ydb::Coordination::RateLimiterCountersMode>(mode);
                        }
                        break;
                    default:
                        Y_ABORT("Unexpected SysParam value %" PRIu64, id);
                }
                if (!sysParamsRowset.Next())
                    return false;
            }
        }

        // Read Sessions
        {
            auto sessionsRowset = db.Table<Schema::Sessions>().Range().Select();
            if (!sessionsRowset.IsReady())
                return false;
            while (!sessionsRowset.EndOfSet()) {
                ui64 sessionId = sessionsRowset.GetValue<Schema::Sessions::Id>();
                ui64 timeoutMillis = sessionsRowset.GetValue<Schema::Sessions::TimeoutMillis>();
                TString description = sessionsRowset.GetValue<Schema::Sessions::Description>();
                TString protectionKey = sessionsRowset.GetValueOrDefault<Schema::Sessions::ProtectionKey>(TString());
                Y_ABORT_UNLESS(sessionId > 0);
                Y_ABORT_UNLESS(!Self->Sessions.contains(sessionId));
                auto* session = &Self->Sessions[sessionId];
                session->Id = sessionId;
                session->TimeoutMillis = timeoutMillis;
                session->Description = std::move(description);
                session->ProtectionKey = std::move(protectionKey);
                Self->TabletCounters->Simple()[COUNTER_SESSION_COUNT].Add(1);
                if (!sessionsRowset.Next())
                    return false;
            }
        }

        // Read Semaphores
        {
            auto semaphoresRowset = db.Table<Schema::Semaphores>().Range().Select();
            if (!semaphoresRowset.IsReady())
                return false;
            while (!semaphoresRowset.EndOfSet()) {
                ui64 semaphoreId = semaphoresRowset.GetValue<Schema::Semaphores::Id>();
                TString name = semaphoresRowset.GetValue<Schema::Semaphores::Name>();
                TString data = semaphoresRowset.GetValue<Schema::Semaphores::Data>();
                ui64 limit = semaphoresRowset.GetValue<Schema::Semaphores::Limit>();
                bool ephemeral = semaphoresRowset.GetValue<Schema::Semaphores::Ephemeral>();
                Y_ABORT_UNLESS(semaphoreId > 0);
                Y_ABORT_UNLESS(!Self->Semaphores.contains(semaphoreId));
                auto* semaphore = &Self->Semaphores[semaphoreId];
                semaphore->Id = semaphoreId;
                semaphore->Name = std::move(name);
                semaphore->Data = std::move(data);
                semaphore->Limit = limit;
                semaphore->Count = 0;
                semaphore->Ephemeral = ephemeral;
                Y_ABORT_UNLESS(!Self->SemaphoresByName.contains(semaphore->Name), "Duplicate semaphore: %s", semaphore->Name.Quote().data());
                Self->SemaphoresByName[semaphore->Name] = semaphore;
                Self->TabletCounters->Simple()[COUNTER_SEMAPHORE_COUNT].Add(1);
                if (!semaphoresRowset.Next())
                    return false;
            }
        }

        // Read SessionSemaphores
        {
            auto sessionSemaphoresRowset = db.Table<Schema::SessionSemaphores>().Range().Select();
            if (!sessionSemaphoresRowset.IsReady())
                return false;
            while (!sessionSemaphoresRowset.EndOfSet()) {
                ui64 sessionId = sessionSemaphoresRowset.GetValue<Schema::SessionSemaphores::SessionId>();
                ui64 semaphoreId = sessionSemaphoresRowset.GetValue<Schema::SessionSemaphores::SemaphoreId>();
                ui64 orderId = sessionSemaphoresRowset.GetValue<Schema::SessionSemaphores::OrderId>();
                ui64 timeoutMillis = sessionSemaphoresRowset.GetValue<Schema::SessionSemaphores::TimeoutMillis>();
                ui64 count = sessionSemaphoresRowset.GetValue<Schema::SessionSemaphores::Count>();
                TString data = sessionSemaphoresRowset.GetValue<Schema::SessionSemaphores::Data>();
                Y_ABORT_UNLESS(sessionId > 0);
                Y_ABORT_UNLESS(Self->Sessions.contains(sessionId), "Missing session: %" PRIu64, sessionId);
                Y_ABORT_UNLESS(semaphoreId > 0);
                Y_ABORT_UNLESS(Self->Semaphores.contains(semaphoreId), "Missing semaphore: %" PRIu64, semaphoreId);
                Y_ABORT_UNLESS(orderId > 0);
                Y_ABORT_UNLESS(count > 0, "Unexpected count: %" PRIu64, count);
                auto* session = &Self->Sessions[sessionId];
                auto* semaphore = &Self->Semaphores[semaphoreId];
                Y_ABORT_UNLESS(!session->WaitingSemaphores.contains(semaphoreId),
                    "Session %" PRIu64 " has duplicate semaphore %s", sessionId, semaphore->Name.Quote().data());
                Y_ABORT_UNLESS(!semaphore->Waiters.contains(orderId),
                    "Semaphore %s has duplicate order id: %" PRIu64, semaphore->Name.Quote().data(), orderId);
                auto* waiter = &session->WaitingSemaphores[semaphoreId];
                waiter->OrderId = orderId;
                waiter->SessionId = sessionId;
                waiter->TimeoutMillis = timeoutMillis;
                waiter->Count = count;
                waiter->Data = data;
                semaphore->Waiters[orderId] = waiter;
                Self->TabletCounters->Simple()[COUNTER_SEMAPHORE_WAITER_COUNT].Add(1);
                if (!sessionSemaphoresRowset.Next())
                    return false;
            }
        }

        // Read QuoterResources
        if (Self->KesusPath) {
            Self->QuoterResources.SetQuoterCounters(GetServiceCounters(AppData()->Counters, "quoter_service")->GetSubgroup("quoter", Self->KesusPath));
            if (Self->RateLimiterCountersMode == Ydb::Coordination::RATE_LIMITER_COUNTERS_MODE_DETAILED) {
                Self->QuoterResources.EnableDetailedCountersMode();
            }
            Self->QuoterResources.SetQuoterPath(Self->KesusPath);
        }

        {
            Self->QuoterResources.SetupBilling(ctx.SelfID, MakeMeteringSink());
            auto quoterResourcesRowset = db.Table<Schema::QuoterResources>().Range().Select();
            if (!quoterResourcesRowset.IsReady())
                return false;
            while (!quoterResourcesRowset.EndOfSet()) {
                const ui64 id = quoterResourcesRowset.GetValue<Schema::QuoterResources::Id>();
                const ui64 parentId = quoterResourcesRowset.GetValue<Schema::QuoterResources::ParentId>();
                NKikimrKesus::TStreamingQuoterResource props = quoterResourcesRowset.GetValue<Schema::QuoterResources::Props>();
                props.SetResourcePath(CanonizeQuoterResourcePath(props.GetResourcePath()));
                Self->QuoterResources.LoadResource(id, parentId, props);
                Self->TabletCounters->Simple()[COUNTER_QUOTER_RESOURCE_COUNT].Add(1);
                if (!quoterResourcesRowset.Next())
                    return false;
            }
            Self->QuoterResources.ConstructTrees();
        }

        Self->PersistSysParam(db, Schema::SysParam_LastLeaderActor, ctx.SelfID.ToString());
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG(ctx, NKikimrServices::KESUS_TABLET, "[%lu] TTxInit::Complete", Self->TabletID());

        if (PreviousTabletActorID && PreviousTabletActorID != ctx.SelfID) {
            // Attempt to kill previous tablet if it is reachable
            ctx.Send(PreviousTabletActorID, new TEvents::TEvPoisonPill());
        }

        // Schedule timeout for all known sessions
        TDuration selfCheckPeriod = Max(Min(Self->SelfCheckPeriod, MAX_SELF_CHECK_PERIOD), MIN_SELF_CHECK_PERIOD);
        TDuration sessionGraceTimeout = Max(Min(Self->SessionGracePeriod, MAX_SESSION_GRACE_PERIOD), selfCheckPeriod + MIN_SESSION_GRACE_PERIOD);
        for (auto& kv : Self->Sessions) {
            auto* session = &kv.second;
            Y_ABORT_UNLESS(Self->ScheduleSessionTimeout(session, ctx, sessionGraceTimeout));
        }

        // Deterministically restore state of all semaphores
        for (auto& kv : Self->Semaphores) {
            ui64 semaphoreId = kv.first;
            auto* semaphore = &kv.second;
            Y_ABORT_UNLESS(!semaphore->Ephemeral || !semaphore->IsEmpty(),
                "Ephemeral semaphore %s restored in an empty state", semaphore->Name.Quote().data());

            TVector<TDelayedEvent> events;
            Self->DoProcessSemaphoreQueue(semaphore, events);
            Y_ABORT_UNLESS(events.empty(),
                "Semaphore %s tried to send events during init", semaphore->Name.Quote().data());

            // Schedule timeout for all other waiters
            for (auto& kv : semaphore->Waiters) {
                auto* waiter = kv.second;
                Y_ABORT_UNLESS(Self->ScheduleWaiterTimeout(semaphoreId, waiter, ctx));
            }
        }

        Self->ScheduleSelfCheck(ctx);
        Self->SignalTabletActive(ctx);
        Self->Become(&TThis::StateWork);
    }
};

NTabletFlatExecutor::ITransaction* TKesusTablet::CreateTxInit() {
    return new TTxInit(this);
}

}
}

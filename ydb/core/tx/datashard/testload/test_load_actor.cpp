#include "actors.h"
#include "info_collector.h"
#include "test_load_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/lib/base/msgbus.h>

#include <google/protobuf/text_format.h>
#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr::NDataShardLoad {

namespace {

// TLoadManager

class TLoadManager : public TActorBootstrapped<TLoadManager> {
    struct TRunningActorInfo {
        TActorId ActorId;
        TActorId Parent; // if set we notify parent when actor finishes

        explicit TRunningActorInfo(const TActorId& actorId, const TActorId& parent = {})
            : ActorId(actorId)
            , Parent(parent)
        {
        }
    };

    struct TFinishedTestInfo {
        ui64 Tag;
        TString ErrorReason;
        TInstant FinishTime;
        std::optional<TEvDataShardLoad::TLoadReport> Report;
    };

    // info about finished actors
    TVector<TFinishedTestInfo> FinishedTests;

    // currently running load actors
    TMap<ui64, TRunningActorInfo> LoadActors;

    ui64 LastTag = 0; // tags start from TagStep

    THashMap<TActorId, ui64> HttpInfoWaiters;
    TActorId HttpInfoCollector;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_LOAD_ACTOR;
    }

    TLoadManager(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
        : Counters(counters)
    {}

    void Bootstrap(const TActorContext& /*ctx*/) {
        Become(&TLoadManager::StateFunc);
    }

    void Handle(TEvDataShardLoad::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        ui32 status = NMsgBusProxy::MSTATUS_OK;
        TString error;
        ui64 tag = 0;
        try {
            tag = ProcessCmd(ev, ctx);
        } catch (const TLoadManagerException& ex) {
            LOG_ERROR_S(ctx, NKikimrServices::DS_LOAD_TEST, "Exception while creating load actor, what# "
                    << ex.what());
            status = NMsgBusProxy::MSTATUS_ERROR;
            error = ex.what();
        }
        auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadResponse>();
        response->Record.SetStatus(status);
        if (error) {
            response->Record.SetErrorReason(error);
        }
        if (record.HasCookie()) {
            response->Record.SetCookie(record.GetCookie());
        }
        if (tag) {
            response->Record.SetTag(tag);
        }
        ctx.Send(ev->Sender, response.release());
    }

    ui64 GetTag() {
        auto tag = ++LastTag;

        // just sanity check
        if (LoadActors.contains(tag)) {
            ythrow TLoadManagerException() << Sprintf("duplicate load actor with Tag# %" PRIu64, tag);
        }

        return tag;
    }

    ui64 ProcessCmd(TEvDataShardLoad::TEvTestLoadRequest::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;
        switch (record.Command_case()) {
        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertBulkStart: {
            const auto& cmd = record.GetUpsertBulkStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new bulk upsert load actor with tag# " << tag);

            auto* actor = CreateUpsertBulkActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));
            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertLocalMkqlStart: {
            const auto& cmd = record.GetUpsertLocalMkqlStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new local mkql upsert load actor with tag# " << tag);

            auto* actor = CreateLocalMkqlUpsertActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertKqpStart: {
            const auto& cmd = record.GetUpsertKqpStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new kqp upsert load actor with tag# " << tag);

            auto* actor = CreateKqpUpsertActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kUpsertProposeStart: {
            const auto& cmd = record.GetUpsertProposeStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new upsert load actor with tag# " << tag);

            auto* actor = CreateProposeUpsertActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kReadIteratorStart: {
            const auto& cmd = record.GetReadIteratorStart();
            const ui64 tag = GetTag();
            LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Create new read iterator load actor# " << tag);

            auto* actor = CreateReadIteratorActor(cmd, ctx.SelfID, GetServiceCounters(Counters, "load_actor"), tag);
            TRunningActorInfo actorInfo(ctx.Register(actor));
            if (record.GetNotifyWhenFinished()) {
                actorInfo.Parent = ev->Sender;
            }
            LoadActors.emplace(tag, std::move(actorInfo));

            return tag;
        }

        case NKikimrDataShardLoad::TEvTestLoadRequest::CommandCase::kLoadStop: {
            const auto& cmd = record.GetLoadStop();
            if (cmd.HasRemoveAllTags() && cmd.GetRemoveAllTags()) {
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete all running load actors");
                for (auto& actorPair : LoadActors) {
                    ctx.Send(actorPair.second.ActorId, new TEvents::TEvPoisonPill);
                }
                LoadActors.clear();
            } else {
                if (!cmd.HasTag()) {
                    ythrow TLoadManagerException() << "Either RemoveAllTags or Tag must present";
                }
                const ui64 tag = cmd.GetTag();
                auto it = LoadActors.find(tag);
                if (it == LoadActors.end()) {
                    ythrow TLoadManagerException()
                        << Sprintf("load actor with Tag# %" PRIu64 " not found", tag);
                }
                LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Delete running load actor# " << tag);
                ctx.Send(it->second.ActorId, new TEvents::TEvPoisonPill);
                LoadActors.erase(it);
            }

            return 0;
        }

        default: {
            TString protoTxt;
            google::protobuf::TextFormat::PrintToString(record, &protoTxt);
            ythrow TLoadManagerException() << (TStringBuilder()
                    << "TLoadManager::Handle(TEvDataShardLoad::TEvTestLoadRequest): unexpected command case: "
                    << ui32(record.Command_case())
                    << " protoTxt# " << protoTxt.Quote());
        }
        }
    }

    void Handle(TEvDataShardLoad::TEvTestLoadFinished::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get();
        auto it = LoadActors.find(msg->Tag);
        Y_VERIFY(it != LoadActors.end(), "%s", (TStringBuilder() << "failed to find actor with tag# " << msg->Tag
            << ", TEvTestLoadFinished from actor# " << ev->Sender).c_str());
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "Load actor# " << ev->Sender
            << " with tag# " << msg->Tag << " finished");

        if (it->second.Parent) {
            auto response = std::make_unique<TEvDataShardLoad::TEvTestLoadFinished>();
            response->Tag = ev->Get()->Tag;
            response->ErrorReason = ev->Get()->ErrorReason;
            response->Report = ev->Get()->Report;
            ctx.Send(it->second.Parent, response.release());
        }

        LoadActors.erase(it);

        FinishedTests.push_back({msg->Tag, msg->ErrorReason, TAppData::TimeProvider->Now(), msg->Report});
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        HttpInfoWaiters[ev->Sender] = ev->Get()->SubRequestId;
        if (HttpInfoCollector) {
            return;
        }

        TVector<TActorId> actors;
        actors.reserve(LoadActors.size());

        // send messages to subactors
        for (const auto& kv : LoadActors) {
            actors.push_back(kv.second.ActorId);
        }

        HttpInfoCollector = ctx.Register(CreateInfoCollector(SelfId(), std::move(actors)));
    }

    void Handle(TEvDataShardLoad::TEvTestLoadInfoResponse::TPtr& ev, const TActorContext& ctx) {
        const auto& record = ev->Get()->Record;

        TStringStream str;
        HTML(str) {
            for (const auto& info: record.GetInfos()) {
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Tag# " << info.GetTag();
                    }
                    DIV_CLASS("panel-body") {
                        str << info.GetData();
                    }
                }
            }

            COLLAPSED_BUTTON_CONTENT("finished_tests_info", "Finished tests") {
                for (const auto& req : FinishedTests) {
                    DIV_CLASS("panel panel-info") {
                        DIV_CLASS("panel-heading") {
                            str << "Tag# " << req.Tag;
                        }
                        DIV_CLASS("panel-body") {
                            str << "<p>";
                            if (req.Report)
                                str << "Report# " << req.Report->ToString() << "<br/>";
                            str << "Finish reason# " << req.ErrorReason << "<br/>";
                            str << "Finish time# " << req.FinishTime << "<br/>";
                            str << "</p>";
                        }
                    }
                }
            }
        }

        for (const auto& it: HttpInfoWaiters) {
            ctx.Send(it.first, new NMon::TEvHttpInfoRes(str.Str(), it.second));
        }

        HttpInfoWaiters.clear();
        HttpInfoCollector = {};
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvDataShardLoad::TEvTestLoadRequest, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadFinished, Handle)
        HFunc(TEvDataShardLoad::TEvTestLoadInfoResponse, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

} // anonymous

IActor *CreateTestLoadActor(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters) {
    return new TLoadManager(counters);
}

} // NKikimr::NDataShardLoad
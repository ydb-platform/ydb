#include "actors.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/datetime/cputimer.h>
#include <util/random/random.h>

#include <google/protobuf/text_format.h>

// * Scheme is hardcoded and it is like default YCSB setup:
// table name is "usertable", 1 utf8 "key" column, 10 utf8 "field0" - "field9" columns
// * row is ~ 1 KB, keys are like user1000385178204227360

namespace NKikimr::NDataShardLoad {

namespace {

class TReadIteratorLoadScenario : public TActorBootstrapped<TReadIteratorLoadScenario> {
    const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart Config;
    const TActorId Parent;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    const ui64 Tag;

    TInstant StartTs;

    TString ConfingString;

public:
    TReadIteratorLoadScenario(const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart& cmd, const TActorId& parent,
            TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
        : Config(cmd)
        , Parent(parent)
        , Counters(std::move(counters))
        , Tag(tag)
    {
        google::protobuf::TextFormat::PrintToString(cmd, &ConfingString);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_NOTICE_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " Bootstrap called: " << ConfingString);

        Become(&TReadIteratorLoadScenario::StateFunc);
    }

private:
    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            str << "ReadIteratorLoadScenario# " << Tag << " started on " << StartTs;
        }
        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    void HandlePoison(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::DS_LOAD_TEST, "ReadIteratorLoadScenario# " << Tag
            << " tablet recieved PoisonPill, going to die");
        Stop(ctx);
    }

    void StopWithError(const TActorContext& ctx, const TString& reason) {
        LOG_WARN_S(ctx, NKikimrServices::DS_LOAD_TEST, "TKqpUpsertActorMultiSession# " << Tag
            << " stopped with error: " << reason);

        ctx.Send(Parent, new TEvDataShardLoad::TEvTestLoadFinished(Tag, reason));
        Stop(ctx);
    }

    void Stop(const TActorContext& ctx) {
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoison)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

} // anonymous

NActors::IActor *CreateReadIteratorActor(const NKikimrDataShardLoad::TEvTestLoadRequest::TReadStart& cmd,
        const NActors::TActorId& parent, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, ui64 tag)
{
    return new TReadIteratorLoadScenario(cmd, parent, std::move(counters), tag);
}

} // NKikimr::NDataShardLoad

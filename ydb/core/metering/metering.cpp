#include "metering.h"

#include <ydb/library/services/services.pb.h>

#include <library/cpp/logger/record.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <util/string/builder.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::METERING_WRITER

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::METERING_WRITER

#if defined LOG_T || \
    defined LOG_D || \
    defined LOG_I || \
    defined LOG_N || \
    defined LOG_W || \
    defined LOG_E
# error log macro redefinition
#endif

namespace NKikimr {
namespace NMetering {

namespace {
using namespace NActors;

////////////////////////////////////////////////////////////////////////////////
class TMeteringWriteActor final
    : public TActor<TMeteringWriteActor>
{
private:
    const THolder<TLogBackend> MeteringFile;
public:
    TMeteringWriteActor(THolder<TLogBackend> meteringFile)
        : TActor(&TThis::StateWork)
          , MeteringFile(std::move(meteringFile))
    {
    }

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::METERING_WRITER_ACTOR;
    }

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteMeteringJson(
        const TEvMetering::TEvWriteMeteringJson::TPtr& ev,
        const TActorContext& ctx);

    void HandleUnexpectedEvent(STFUNC_SIG);
};

////////////////////////////////////////////////////////////////////////////////
void TMeteringWriteActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    Die(ctx);
}

STFUNC(TMeteringWriteActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvMetering::TEvWriteMeteringJson, HandleWriteMeteringJson);
    default:
        HandleUnexpectedEvent(ev);
        break;
    }
}

void TMeteringWriteActor::HandleWriteMeteringJson(
    const TEvMetering::TEvWriteMeteringJson::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    const auto* msg = ev->Get();
    try {
        MeteringFile->WriteData(
            TLogRecord(
                ELogPriority::TLOG_INFO,
                msg->MeteringJson.data(),
                msg->MeteringJson.length()));
    } catch (const TFileError& e) {
        YDB_LOG_WARN("TMeteringWriteActor: unable to write metering data",
            {"(error", e.what()});
    }
}

void TMeteringWriteActor::HandleUnexpectedEvent(STFUNC_SIG)
{
    YDB_LOG_WARN("TMeteringWriteActor: unhandled event",
        {"type", ev->GetTypeRewrite()},
        {"event", ev->ToString()});
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////
THolder<NActors::IActor> CreateMeteringWriter(THolder<TLogBackend> meteringFile)
{
    return MakeHolder<TMeteringWriteActor>(std::move(meteringFile));
}


void SendMeteringJson(const NActors::TActorContext &ctx, TString message)
{
    auto request = MakeHolder<TEvMetering::TEvWriteMeteringJson>(std::move(message));
    ctx.Send(
        MakeMeteringServiceID(),
        request.Release());
}

}    // namespace NKikime
}    // namespace NMetering


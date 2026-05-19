#include "service_actor.h"

#include <ydb/core/base/appdata.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BS_LOAD_TEST


namespace NKikimr {

class TMemoryLoadTestActor : public TActorBootstrapped<TMemoryLoadTestActor> {
    enum {
        EvAllocateBlock = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvEnd
    };

    struct TEvAllocateBlock : public TEventLocal<TEvAllocateBlock, EvAllocateBlock> {};

    const TActorId Parent;
    const ui64 Tag;

    TDuration Duration;
    ui32 DurationSeconds;
    ui64 BlockSize;
    TDuration Interval;

    TInstant TestStartTime;
    bool EarlyStop = false;
    TVector<TVector<char>> Blocks;
    ui64 AllocatedSize = 0;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_PDISK_LOG_WRITE;
    }

    TMemoryLoadTestActor(const NKikimr::TEvLoadTestRequest::TMemoryLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
    {
        Y_UNUSED(counters);
        Y_UNUSED(index);

        VERIFY_PARAM(DurationSeconds);
        Duration = TDuration::Seconds(cmd.GetDurationSeconds());
        DurationSeconds = cmd.GetDurationSeconds();

        VERIFY_PARAM(BlockSize);
        BlockSize = cmd.GetBlockSize();

        VERIFY_PARAM(IntervalUs);
        Interval = TDuration::MicroSeconds(cmd.GetIntervalUs());

        Blocks.reserve(Duration.MicroSeconds() / Interval.MicroSeconds() + 1);
    }

    void Bootstrap(const TActorContext& ctx) {
        YDB_LOG_CTX_DEBUG(ctx, "TMemoryLoadTestActor Bootstrap called",
            {"Tag", Tag});

        Become(&TMemoryLoadTestActor::StateFunc);

        YDB_LOG_CTX_INFO(ctx, "Schedule PoisonPill",
            {"Tag", Tag});

        ctx.Schedule(Duration, new TEvents::TEvPoisonPill);
        ctx.Schedule(Interval, new TEvAllocateBlock);
        TestStartTime = TAppData::TimeProvider->Now();
        EarlyStop = false;
    }

    void HandlePoisonPill(const TActorContext& ctx) {
        EarlyStop = (TAppData::TimeProvider->Now() - TestStartTime).Seconds() < DurationSeconds;
        YDB_LOG_CTX_INFO(ctx, "Handle PoisonPill",
            {"Tag", Tag});

        TIntrusivePtr<TEvLoad::TLoadReport> report = nullptr;
        if (!EarlyStop) {
            report.Reset(new TEvLoad::TLoadReport());
            report->Duration = Duration;
        }
        const TString errorReason = EarlyStop ?
            "Abort, stop signal received" : "OK, called StartDeathProcess";
        ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, report, errorReason));
        Die(ctx);
    }

    void Handle(TEvAllocateBlock::TPtr&, const TActorContext& ctx) {
        auto size = RandomNumber<ui64>(BlockSize * 2 + 1);

        Blocks.push_back({});
        auto& block = Blocks.back();
        block.resize(size);
        for (size_t i = 0; i < size; ++i) {
            block[i] = 0;
        }
        AllocatedSize += size;

        YDB_LOG_CTX_DEBUG(ctx, "Handle AllocateBlock",
            {"Tag", Tag});

        ctx.Schedule(Interval, new TEvAllocateBlock);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
#define PARAM(NAME, VALUE) \
    TABLER() { \
        TABLED() { str << NAME; } \
        TABLED() { str << VALUE; } \
    }
        TStringStream str;
        HTML(str) {
            TABLE_CLASS("table table-condensed") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Parameter"; }
                        TABLEH() { str << "Value"; }
                    }
                }
                TABLEBODY() {
                    PARAM("Elapsed time / Duration",
                        (TAppData::TimeProvider->Now() - TestStartTime).Seconds() << "s / " << DurationSeconds << "s");
                    PARAM("Interval", Interval.MicroSeconds() << "us");
                    PARAM("Block size", BlockSize);
                    PARAM("Allocated bytes", AllocatedSize);
                    PARAM("Allocated blocks", Blocks.size());
                }
            }
        }
#undef PARAM

        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(TEvAllocateBlock, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

IActor* CreateMemoryLoadTest(
    const NKikimr::TEvLoadTestRequest::TMemoryLoad& cmd,
    const TActorId& parent,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
    ui64 index,
    ui64 tag)
{
    return new TMemoryLoadTestActor(cmd, parent, counters, index, tag);
}

} // NKikimr

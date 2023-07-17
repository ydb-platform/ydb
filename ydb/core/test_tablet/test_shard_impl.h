#pragma once

#include "defs.h"
#include "events.h"
#include "test_shard_context.h"

namespace NKikimr::NTestShard {

    struct TExDie : std::exception {};

    using NTabletFlatExecutor::ITransaction;
    using NTabletFlatExecutor::TTransactionBase;

    class TTestShard : public NKeyValue::TKeyValueFlat {
        std::optional<NKikimrClient::TTestShardControlRequest::TCmdInitialize> Settings;
        std::unique_ptr<TTabletCountersBase> Counters;

    public:
        enum class EMode {
            WRITE,
            READ_VALIDATE,
            STATE_SERVER_CONNECT,
            INITIAL,
        };

        enum {
            EvSwitchMode = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        struct TEvSwitchMode : TEventLocal<TEvSwitchMode, EvSwitchMode> {
            EMode Mode;
            TEvSwitchMode(EMode mode) : Mode(mode) {}
        };

    public:
        TTestShard(const TActorId& tablet, TTabletStorageInfo *info)
            : TKeyValueFlat(tablet, info)
        {
            using TKeyValueCounters = TProtobufTabletCounters<
                NKeyValue::ESimpleCounters_descriptor,
                NKeyValue::ECumulativeCounters_descriptor,
                NKeyValue::EPercentileCounters_descriptor,
                NKeyValue::ETxTypes_descriptor>;

            using TTestShardCounters = TAppProtobufTabletCounters<
                NTestShard::ESimpleCounters_descriptor,
                NTestShard::ECumulativeCounters_descriptor,
                NTestShard::EPercentileCounters_descriptor>;

            TProtobufTabletCountersPair<TKeyValueCounters, TTestShardCounters> pair;
            State.SetupTabletCounters(pair.GetFirstTabletCounters().Release());
            Counters.reset(pair.GetSecondTabletCounters().Release());
            Counters->Simple()[NTestShard::COUNTER_MODE_INITIAL] = 1;
        }

        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TEST_SHARD_ACTOR;
        }

        void CreatedHook(const TActorContext& ctx) override {
            Execute(CreateTxInitScheme(), ctx);
        }

        bool HandleHook(STFUNC_SIG) override {
            SetActivityType(NKikimrServices::TActivity::TEST_SHARD_ACTOR);
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvControlRequest, Handle);
                HFunc(TEvSwitchMode, Handle);
                default:
                    return false;
            }
            return true;
        }

        void Handle(TEvControlRequest::TPtr ev, const TActorContext& ctx) {
            const auto& record = ev->Get()->Record;
            switch (record.GetCommandCase()) {
                case NKikimrClient::TTestShardControlRequest::kInitialize:
                    Execute(CreateTxInitialize(record.GetInitialize(), ev->Sender, ev->Cookie), ctx);
                    break;

                case NKikimrClient::TTestShardControlRequest::COMMAND_NOT_SET:
                    break;
            }
        }

        void Handle(TEvSwitchMode::TPtr ev, const TActorContext& /*ctx*/) {
            const EMode mode = ev->Get()->Mode;
            auto& s = Counters->Simple();
            s[NTestShard::COUNTER_MODE_WRITE] = mode == EMode::WRITE;
            s[NTestShard::COUNTER_MODE_READ_VALIDATE] = mode == EMode::READ_VALIDATE;
            s[NTestShard::COUNTER_MODE_STATE_SERVER_CONNECT] = mode == EMode::STATE_SERVER_CONNECT;
            s[NTestShard::COUNTER_MODE_INITIAL] = mode == EMode::INITIAL;
        }

        void OnLoadComplete() {
            SignalTabletActive(SelfId());
            StartActivities();
        }

        bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override {
            if (!ev) {
                return true;
            }
            const auto& cgi = ev->Get()->Cgi();
            if (cgi.Get("page") == "keyvalue") {
                return TKeyValueFlat::OnRenderAppHtmlPage(ev, ctx);
            } else {
                Register(CreateMonQueryActor(ev));
                return true;
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        TActorId ActivityActorId;

        void StartActivities();
        void PassAway() override;

    private:
        class TTxInitialize;
        friend class TTxInitialize;
        ITransaction *CreateTxInitialize(const NKikimrClient::TTestShardControlRequest::TCmdInitialize& cmd,
            TActorId sender, ui64 cookie);

        class TTxInitScheme;
        friend class TTxInitScheme;
        ITransaction *CreateTxInitScheme();

        class TTxLoadEverything;
        friend class TTxLoadEverything;
        ITransaction *CreateTxLoadEverything();

        class TMonQueryActor;
        IActor *CreateMonQueryActor(NMon::TEvRemoteHttpInfo::TPtr ev);
    };

} // NKikimr::NTestShard

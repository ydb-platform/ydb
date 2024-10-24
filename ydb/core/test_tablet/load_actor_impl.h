#pragma once

#include "defs.h"

#include "test_shard_impl.h"
#include "time_series.h"
#include "state_server_interface.h"

namespace NKikimr::NTestShard {

    class TLoadActor : public TActorBootstrapped<TLoadActor> {
        const ui64 TabletId;
        const ui32 Generation;
        const TActorId Tablet;
        TActorId TabletActorId;
        const NKikimrClient::TTestShardControlRequest::TCmdInitialize Settings;

        ui64 ValidationRunningCount = 0;

        struct TKeyInfo {
            const ui32 Len = 0;
            ::NTestShard::TStateServer::EEntityState ConfirmedState = ::NTestShard::TStateServer::ABSENT;
            ::NTestShard::TStateServer::EEntityState PendingState = ::NTestShard::TStateServer::ABSENT;
            std::unique_ptr<TEvKeyValue::TEvRequest> Request;
            NWilson::TTraceId TraceId;
            size_t ConfirmedKeyIndex = Max<size_t>();

            TKeyInfo(ui32 len)
                : Len(len)
            {}

            ~TKeyInfo() {
                Y_ABORT_UNLESS(ConfirmedKeyIndex == Max<size_t>());
            }
        };

        enum {
            EvValidationFinished = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvDoSomeAction,
            EvWriteOnTime,
        };

        struct TEvValidationFinished : TEventLocal<TEvValidationFinished, EvValidationFinished> {
            std::unordered_map<TString, TKeyInfo> Keys;
            bool InitialCheck;

            TEvValidationFinished(std::unordered_map<TString, TKeyInfo> keys, bool initialCheck)
                : Keys(std::move(keys))
                , InitialCheck(initialCheck)
            {}
        };

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::TEST_SHARD_LOAD_ACTOR;
        }

        TLoadActor(ui64 tabletId, ui32 generation, const TActorId tablet,
            const NKikimrClient::TTestShardControlRequest::TCmdInitialize& settings);
        ~TLoadActor();
        void ClearKeys();
        void Bootstrap(const TActorId& parentId);
        void PassAway() override;
        void HandleWakeup();
        void Action();
        void Handle(TEvStateServerStatus::TPtr ev);

        STRICT_STFUNC(StateFunc,
            hFunc(TEvKeyValue::TEvResponse, Handle);
            hFunc(NMon::TEvRemoteHttpInfo, Handle);
            hFunc(TEvStateServerStatus, Handle);
            hFunc(TEvStateServerWriteResult, Handle);
            hFunc(TEvValidationFinished, Handle);
            cFunc(EvDoSomeAction, HandleDoSomeAction);
            cFunc(EvWriteOnTime, HandleWriteOnTime);
            cFunc(TEvents::TSystem::Poison, PassAway);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        )

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Generic request/response management

        ui64 BytesProcessed = 0;
        ui32 StallCounter = 0;
        ui64 LastCookie = 0;

        std::unique_ptr<TEvKeyValue::TEvRequest> CreateRequest();
        void Handle(TEvKeyValue::TEvResponse::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Key state

        std::unordered_map<TString, TKeyInfo> Keys;
        std::vector<TString> ConfirmedKeys;

        using TKey = std::unordered_map<TString, TKeyInfo>::value_type;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // State validation actor

        class TValidationActor;
        friend class TValidationActor;

        TActorId ValidationActorId;
        void RunValidation(bool initialCheck);
        void Handle(TEvValidationFinished::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // KV tablet write management code

        struct TWriteInfo {
            THPTimer Timer; // reset when write request is issued
            std::vector<TString> KeysInQuery;

            TWriteInfo(TString key)
                : KeysInQuery(1, std::move(key))
            {}
        };

        ui64 BytesOfData = 0;

        std::unordered_map<ui64, TWriteInfo> WritesInFlight; // cookie -> TWriteInfo
        std::unordered_map<ui64, TString> PatchesInFlight;
        ui32 KeysWritten = 0;
        static constexpr TDuration WriteSpeedWindow = TDuration::Seconds(10);
        static constexpr TDuration ReadSpeedWindow = TDuration::Seconds(10);
        static constexpr TDuration StateServerWriteLatencyWindow = TDuration::Seconds(10);
        static constexpr TDuration WriteLatencyWindow = TDuration::Seconds(10);
        static constexpr TDuration ReadLatencyWindow = TDuration::Seconds(10);
        TSpeedMeter WriteSpeed{WriteSpeedWindow};
        TSpeedMeter ReadSpeed{ReadSpeedWindow};
        TTimeSeries StateServerWriteLatency{StateServerWriteLatencyWindow};
        TTimeSeries WriteLatency{WriteLatencyWindow};
        TTimeSeries ReadLatency{ReadLatencyWindow};
        TMonotonic NextWriteTimestamp;
        bool WriteOnTimeScheduled = false;
        bool DoSomeActionInFlight = false;

        void GenerateKeyValue(TString *key, TString *value, bool *isInline);
        void IssueWrite();
        void IssuePatch();
        void ProcessWriteResult(ui64 cookie, const google::protobuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TWriteResult>& results);
        void ProcessPatchResult(ui64 cookie, const google::protobuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TPatchResult>& results);
        void TrimBytesWritten(TInstant now);
        void HandleWriteOnTime();
        void HandleDoSomeAction();

        std::unordered_map<ui64, std::tuple<TString, TMonotonic, bool, std::vector<std::tuple<ui32, ui32>>>> ReadsInFlight;
        std::unordered_map<TString, ui32> KeysBeingRead;

        bool IssueRead();
        void ProcessReadResult(ui64 cookie, const NProtoBuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TReadResult>& results,
            TEvKeyValue::TEvResponse& event);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // KV tablet delete management code

        struct TDeleteInfo {
            std::vector<TString> KeysInQuery; // keys being deleted

            TDeleteInfo(TString key)
                : KeysInQuery(1, std::move(key))
            {}
        };

        std::unordered_map<ui64, TDeleteInfo> DeletesInFlight; // cookie -> TDeleteInfo
        ui32 KeysDeleted = 0;

        void IssueDelete();
        void ProcessDeleteResult(ui64 cookie, const google::protobuf::RepeatedPtrField<NKikimrClient::TKeyValueResponse::TDeleteRangeResult>& results);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // State management

        std::deque<TKey*> TransitionInFlight;

        void RegisterTransition(TKey& key, ::NTestShard::TStateServer::EEntityState from,
            ::NTestShard::TStateServer::EEntityState to, std::unique_ptr<TEvKeyValue::TEvRequest> ev = nullptr,
            NWilson::TTraceId traceId = {});
        void Handle(TEvStateServerWriteResult::TPtr ev);

        void MakeConfirmed(TKey& key);
        void MakeUnconfirmed(TKey& key);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Tablet monitoring

        void Handle(NMon::TEvRemoteHttpInfo::TPtr ev);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Random generators

        template<typename T>
        size_t PickInterval(const google::protobuf::RepeatedPtrField<T>& intervals) {
            std::vector<ui64> cw;
            ui64 w = 0;
            cw.reserve(intervals.size());
            for (const auto& interval : intervals) {
                Y_ABORT_UNLESS(interval.HasWeight());
                Y_ABORT_UNLESS(interval.GetWeight());
                w += interval.GetWeight();
                cw.push_back(w);
            }
            const size_t num = std::upper_bound(cw.begin(), cw.end(), RandomNumber(w)) - cw.begin();
            Y_ABORT_UNLESS(num < cw.size());
            return num;
        }

        TDuration GenerateRandomInterval(const NKikimrClient::TTestShardControlRequest::TTimeInterval& interval);
        TDuration GenerateRandomInterval(const google::protobuf::RepeatedPtrField<NKikimrClient::TTestShardControlRequest::TTimeInterval>& intervals);
        size_t GenerateRandomSize(const google::protobuf::RepeatedPtrField<NKikimrClient::TTestShardControlRequest::TSizeInterval>& intervals,
                bool *isInline);
    };

} // NKikimr::NTestShard

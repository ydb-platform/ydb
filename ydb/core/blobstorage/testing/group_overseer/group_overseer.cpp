#include "group_overseer.h"
#include "group_state.h"

namespace NKikimr::NTesting {

    class TGroupOverseer::TImpl {
        std::unordered_map<TActorId, ui32> OverseenServiceMap;
        std::unordered_map<ui32, TGroupState> GroupStates;
        std::unordered_map<TQueryId, ui32> QueryToGroup;

    public:
        void AddGroupToOversee(ui32 groupId) {
            const TActorId proxyId = MakeBlobStorageProxyID(groupId);
            OverseenServiceMap[proxyId] = groupId;
            GroupStates.try_emplace(groupId, groupId);
        }

        void ExamineEvent(ui32 nodeId, IEventHandle& ev) {
            switch (ev.GetTypeRewrite()) { // all of these events may alter storage state
#define QUERY(EV) case TEvBlobStorage::EV: return ExamineResultEvent<TEvBlobStorage::T##EV>(nodeId, ev);
                QUERY(EvBlockResult)
                QUERY(EvPutResult)
                QUERY(EvPatchResult)
                QUERY(EvInplacePatchResult)
                QUERY(EvCollectGarbageResult)
                QUERY(EvGetResult)
                QUERY(EvRangeResult)
                QUERY(EvDiscoverResult)
#undef QUERY
            }
        }

        void ExamineEnqueue(ui32 nodeId, IEventHandle& ev) {
            switch (ev.GetTypeRewrite()) { // all of these events may alter storage state
#define RESULT(EV) case TEvBlobStorage::EV: return ExamineQueryEvent<TEvBlobStorage::T##EV>(nodeId, ev, TEvBlobStorage::EV##Result);
                RESULT(EvBlock)
                RESULT(EvPut)
                RESULT(EvPatch)
                RESULT(EvInplacePatch)
                RESULT(EvCollectGarbage)
                RESULT(EvGet)
                RESULT(EvRange)
                RESULT(EvDiscover)
#undef RESULT
            }
        }

        EBlobState GetBlobState(ui32 groupId, TLogoBlobID id) const {
            if (const auto it = GroupStates.find(groupId); it != GroupStates.end()) {
                return it->second.GetBlobState(id);
            } else {
                Y_FAIL_S("GroupId# " << groupId << " is not being overseen");
            }
        }

        void EnumerateBlobs(ui32 groupId, const std::function<void(TLogoBlobID, EBlobState)>& callback) const {
            if (const auto it = GroupStates.find(groupId); it != GroupStates.end()) {
                return it->second.EnumerateBlobs(callback);
            } else {
                Y_ABORT();
            }
        }

    private:
        template<typename T>
        void ExamineQueryEvent(ui32 nodeId, IEventHandle& ev, ui32 resultEventType) {
            Y_ABORT_UNLESS(ev.GetTypeRewrite() == T::EventType);

            const auto it = OverseenServiceMap.find(ev.Recipient);
            if (it == OverseenServiceMap.end()) {
                return;
            }
            const ui32 groupId = it->second;

            const TQueryId queryId{nodeId, resultEventType, ev.Sender, ev.Cookie};
            const auto [_, inserted] = QueryToGroup.emplace(queryId, groupId);
            if (inserted) {
                const auto groupStateIt = GroupStates.try_emplace(groupId, groupId).first;
                groupStateIt->second.ExamineQueryEvent(queryId, *ev.Get<T>());
            }
        }

        template<typename T>
        void ExamineResultEvent(ui32 nodeId, IEventHandle& ev) {
            Y_ABORT_UNLESS(ev.GetTypeRewrite() == T::EventType);

            const TQueryId queryId{nodeId, T::EventType, ev.Recipient, ev.Cookie};
            const auto it = QueryToGroup.find(queryId);
            if (it == QueryToGroup.end()) {
                return;
            }
            const ui32 groupId = it->second;
            QueryToGroup.erase(it);

            auto& msg = *ev.Get<T>();

            if constexpr (T::EventType == TEvBlobStorage::EvGetResult || T::EventType == TEvBlobStorage::EvPutResult ||
                    T::EventType == TEvBlobStorage::EvRangeResult) {
                Y_ABORT_UNLESS(groupId == msg.GroupId);
            }
            else if constexpr (T::EventType != TEvBlobStorage::EvBlockResult &&
                    T::EventType != TEvBlobStorage::EvInplacePatchResult &&
                    T::EventType != TEvBlobStorage::EvCollectGarbageResult &&
                    T::EventType != TEvBlobStorage::EvDiscoverResult) {
                Y_ABORT_UNLESS(groupId == msg.GroupId.GetRawId());
            }

            const auto groupStateIt = GroupStates.try_emplace(groupId, groupId).first;
            groupStateIt->second.ExamineResultEvent(queryId, msg);
        }
    };

    TGroupOverseer::TGroupOverseer()
        : Impl(std::make_unique<TImpl>())
    {}

    TGroupOverseer::~TGroupOverseer()
    {}

    void TGroupOverseer::AddGroupToOversee(ui32 groupId) {
        Impl->AddGroupToOversee(groupId);
    }

    void TGroupOverseer::ExamineEnqueue(ui32 nodeId, IEventHandle& ev) {
        Impl->ExamineEnqueue(nodeId, ev);
    }

    void TGroupOverseer::ExamineEvent(ui32 nodeId, IEventHandle& ev) {
        Impl->ExamineEvent(nodeId, ev);
    }

    EBlobState TGroupOverseer::GetBlobState(ui32 groupId, TLogoBlobID id) const {
        return Impl->GetBlobState(groupId, id);
    }

    void TGroupOverseer::EnumerateBlobs(ui32 groupId, const std::function<void(TLogoBlobID, EBlobState)>& callback) const {
        Impl->EnumerateBlobs(groupId, callback);
    }

} // NKikimr::NTesting

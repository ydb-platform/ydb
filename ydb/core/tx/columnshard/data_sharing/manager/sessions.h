#pragma once
#include <ydb/core/tx/columnshard/data_sharing/source/session/source.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/session/destination.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap::NDataSharing {

class TSessionsManager {
private:
    THashMap<TString, std::shared_ptr<TSourceSession>> SourceSessions;
    THashMap<TString, std::shared_ptr<TDestinationSession>> DestSessions;
    TAtomicCounter SharingSessions;
public:
    TSessionsManager() = default;

    void StartSharingSession() {
        SharingSessions.Inc();
    }

    void FinishSharingSession() {
        AFL_VERIFY(SharingSessions.Dec() >= 0);
    }

    bool IsSharingInProgress() const {
        return SharingSessions.Val();
    }

    void Start(const NColumnShard::TColumnShard& shard) const;

    std::shared_ptr<TSourceSession> GetSourceSession(const TString& sessionId) const {
        auto it = SourceSessions.find(sessionId);
        if (it == SourceSessions.end()) {
            return nullptr;
        }
        return it->second;
    }

    std::shared_ptr<TDestinationSession> GetDestinationSession(const TString& sessionId) const {
        auto it = DestSessions.find(sessionId);
        if (it == DestSessions.end()) {
            return nullptr;
        }
        return it->second;
    }

    void RemoveSourceSession(const TString& sessionId) {
        SourceSessions.erase(sessionId);
    }

    void RemoveDestinationSession(const TString& sessionId) {
        DestSessions.erase(sessionId);
    }

    [[nodiscard]] bool Load(NTable::TDatabase& database, const TColumnEngineForLogs* index);

    void InitializeEventsExchange(const NColumnShard::TColumnShard& shard, const std::optional<ui64> sessionCookie = {});

    std::unique_ptr<NTabletFlatExecutor::ITransaction> InitializeSourceSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& session);
    std::unique_ptr<NTabletFlatExecutor::ITransaction> ProposeDestSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session);
    std::unique_ptr<NTabletFlatExecutor::ITransaction> ConfirmDestSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session);

};

}
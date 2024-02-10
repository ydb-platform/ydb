#include "sessions.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/transactions/tx_start_from_initiator.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_start_to_source.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NOlap::NDataSharing {

void TSessionsManager::InitializeEventsExchange(const NColumnShard::TColumnShard& shard, const std::optional<ui64> sessionCookie) {
    AFL_VERIFY(!sessionCookie || *sessionCookie);
    for (auto&& i : SourceSessions) {
        if (sessionCookie && *sessionCookie != i.second->GetRuntimeId()) {
            continue;
        }
        i.second->ActualizeDestination();
    }
    for (auto&& i : DestSessions) {
        if (sessionCookie && *sessionCookie != i.second->GetRuntimeId()) {
            continue;
        }
        i.second->SendCurrentCursorAck(shard, {});
    }
}

bool TSessionsManager::Load(NTable::TDatabase& database, const TColumnEngineForLogs* index, const std::shared_ptr<TSharedBlobsManager>& sharedBlobsManager) {
    NIceDb::TNiceDb db(database);
    using namespace NColumnShard;
    {
        auto rowset = db.Table<Schema::SourceSessions>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const TString& sessionId = rowset.GetValue<Schema::SourceSessions::SessionId>();
            auto session = std::make_shared<TSourceSession>((TTabletId)index->GetTabletId());

            NKikimrColumnShardDataSharingProto::TSourceSession protoSession;
            AFL_VERIFY(protoSession.ParseFromString(rowset.GetValue<Schema::SourceSessions::Details>()));

            NKikimrColumnShardDataSharingProto::TSourceSession::TCursor protoSessionCursor;
            AFL_VERIFY(protoSessionCursor.ParseFromString(rowset.GetValue<Schema::SourceSessions::Cursor>()));

            AFL_VERIFY(index);
            AFL_VERIFY(session->DeserializeFromProto(protoSession, protoSessionCursor, *index, sharedBlobsManager));
            AFL_VERIFY(SourceSessions.emplace(session->GetSessionId(), session).second);
            if (!rowset.Next()) {
                return false;
            }
        }

    }

    {
        auto rowset = db.Table<Schema::DestinationSessions>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const TString& sessionId = rowset.GetValue<Schema::DestinationSessions::SessionId>();
            auto session = std::make_shared<TDestinationSession>();

            NKikimrColumnShardDataSharingProto::TDestinationSession protoSession;
            AFL_VERIFY(protoSession.ParseFromString(rowset.GetValue<Schema::DestinationSessions::Details>()));

            NKikimrColumnShardDataSharingProto::TDestinationSession::TFullCursor protoSessionCursor;
            AFL_VERIFY(protoSessionCursor.ParseFromString(rowset.GetValue<Schema::DestinationSessions::Cursor>()));

            AFL_VERIFY(index);
            AFL_VERIFY(session->DeserializeDataFromProto(protoSession, *index));
            AFL_VERIFY(session->DeserializeCursorFromProto(protoSessionCursor));

            if (!rowset.Next()) {
                return false;
            }
        }
    }
    return true;
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TSessionsManager::StartDestSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session) {
    AFL_VERIFY(session);
    AFL_VERIFY(DestSessions.emplace(session->GetSessionId(), session).second);
    return std::make_unique<TTxStartFromInitiator>(self, session);
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TSessionsManager::StartSourceSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& session) {
    AFL_VERIFY(session);
    AFL_VERIFY(SourceSessions.emplace(session->GetSessionId(), session).second);
    return std::make_unique<TTxStartToSource>(self, session);
}

}
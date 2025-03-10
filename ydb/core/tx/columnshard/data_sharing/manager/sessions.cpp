#include "sessions.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/data_sharing/destination/transactions/tx_start_from_initiator.h>
#include <ydb/core/tx/columnshard/data_sharing/source/transactions/tx_start_to_source.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

namespace NKikimr::NOlap::NDataSharing {

void TSessionsManager::Start(NColumnShard::TColumnShard& shard) const {
    NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build()("sessions", "start")("tablet_id", shard.TabletID());
    for (auto&& i : SourceSessions) {
        if (i.second->IsReadyForStarting()) {
            i.second->PrepareToStart(shard);
        }
    }
    for (auto&& i : DestSessions) {
        if (i.second->IsReadyForStarting() && i.second->IsConfirmed()) {
            i.second->PrepareToStart(shard);
        }
    }

    for (auto&& i : SourceSessions) {
        if (i.second->IsPrepared()) {
            TConclusionStatus status = i.second->TryStart(shard);
            AFL_VERIFY(status.Ok())("failed to start source session", status.GetErrorMessage());
        }
    }
    for (auto&& i : DestSessions) {
        if (i.second->IsPrepared() && i.second->IsConfirmed()) {
            TConclusionStatus status = i.second->TryStart(shard);
            AFL_VERIFY(status.Ok())("failed to start dest session", status.GetErrorMessage());

            if (!i.second->GetSourcesInProgressCount()) {
                i.second->Finish(shard, shard.GetDataLocksManager());
            }
        }
    }
    NYDBTest::TControllers::GetColumnShardController()->OnAfterSharingSessionsManagerStart(shard);
}

void TSessionsManager::InitializeEventsExchange(const NColumnShard::TColumnShard& shard, const std::optional<ui64> sessionCookie) {
    AFL_VERIFY(!sessionCookie || *sessionCookie);
    for (auto&& i : SourceSessions) {
        if (sessionCookie && *sessionCookie != i.second->GetRuntimeId()) {
            continue;
        }
        i.second->ActualizeDestination(shard, shard.GetDataLocksManager());
    }
    for (auto&& i : DestSessions) {
        if (sessionCookie && *sessionCookie != i.second->GetRuntimeId()) {
            continue;
        }
        i.second->SendCurrentCursorAck(shard, {});
    }
}

bool TSessionsManager::Load(NTable::TDatabase& database, const TColumnEngineForLogs* index) {
    NIceDb::TNiceDb db(database);
    using namespace NColumnShard;
    {
        auto rowset = db.Table<Schema::SourceSessions>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            auto session = std::make_shared<TSourceSession>((TTabletId)index->GetTabletId());

            NKikimrColumnShardDataSharingProto::TSourceSession protoSession;
            AFL_VERIFY(protoSession.ParseFromString(rowset.GetValue<Schema::SourceSessions::Details>()));

            std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic> protoSessionCursorDynamic;
            if (rowset.HaveValue<Schema::SourceSessions::CursorDynamic>()) {
                protoSessionCursorDynamic = NKikimrColumnShardDataSharingProto::TSourceSession::TCursorDynamic{};
                AFL_VERIFY(protoSessionCursorDynamic->ParseFromString(rowset.GetValue<Schema::SourceSessions::CursorDynamic>()));
            }

            std::optional<NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic> protoSessionCursorStatic;
            if (rowset.HaveValue<Schema::SourceSessions::CursorStatic>()) {
                protoSessionCursorStatic = NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic{};
                AFL_VERIFY(protoSessionCursorStatic->ParseFromString(rowset.GetValue<Schema::SourceSessions::CursorStatic>()));
            }

            if (protoSessionCursorDynamic && !protoSessionCursorStatic) {
                protoSessionCursorStatic = NKikimrColumnShardDataSharingProto::TSourceSession::TCursorStatic{};
            }

            AFL_VERIFY(index);
            session->DeserializeFromProto(protoSession, protoSessionCursorDynamic, protoSessionCursorStatic).Validate();
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
            auto session = std::make_shared<TDestinationSession>();

            NKikimrColumnShardDataSharingProto::TDestinationSession protoSession;
            AFL_VERIFY(protoSession.ParseFromString(rowset.GetValue<Schema::DestinationSessions::Details>()));

            NKikimrColumnShardDataSharingProto::TDestinationSession::TFullCursor protoSessionCursor;
            AFL_VERIFY(protoSessionCursor.ParseFromString(rowset.GetValue<Schema::DestinationSessions::Cursor>()));

            AFL_VERIFY(index);
            session->DeserializeDataFromProto(protoSession, *index).Validate();
            session->DeserializeCursorFromProto(protoSessionCursor).Validate();
            AFL_VERIFY(DestSessions.emplace(session->GetSessionId(), session).second);
            if (!rowset.Next()) {
                return false;
            }
        }
    }
    return true;
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TSessionsManager::ProposeDestSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session) {
    AFL_VERIFY(session);
    return std::make_unique<TTxProposeFromInitiator>(self, session, DestSessions, "tx_propose_from_initiator");
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TSessionsManager::ConfirmDestSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TDestinationSession>& session) {
    AFL_VERIFY(session);
    return std::make_unique<TTxConfirmFromInitiator>(self, session, "tx_confirm_from_initiator");
}

std::unique_ptr<NTabletFlatExecutor::ITransaction> TSessionsManager::InitializeSourceSession(NColumnShard::TColumnShard* self, const std::shared_ptr<TSourceSession>& session) {
    AFL_VERIFY(session);
    return std::make_unique<TTxStartToSource>(self, session, SourceSessions, "tx_start_to_source");
}

}
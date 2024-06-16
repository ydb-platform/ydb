#pragma once
#include <ydb/core/tx/columnshard/export/session/session.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap::NExport {

    class TExportsManager {
    private:
        THashMap<TIdentifier, std::shared_ptr<TSession>> Sessions;
    public:
        void Start(const NColumnShard::TColumnShard* shard);
        void Stop();

        TConclusionStatus ProposeTask(const std::shared_ptr<TExportTask>& exportTask) {
            auto it = Sessions.find(exportTask->GetIdentifier());
            if (it != Sessions.end()) {
                return TConclusionStatus::Fail("task identifier exists already");
            }
            Sessions.emplace(exportTask->GetIdentifier(), std::make_shared<TSession>(exportTask));
            return TConclusionStatus::Success();
        }

        bool ConfirmSessionOnExecute(const NExport::TIdentifier& id, NTabletFlatExecutor::TTransactionContext& txc) {
            auto session = GetSessionVerified(id);
            AFL_VERIFY(session->IsDraft());
            session->SaveFullToDB(txc.DB);
            return true;
        }

        bool ConfirmSessionOnComplete(const NExport::TIdentifier& id) {
            GetSessionVerified(id)->Confirm();
            return true;
        }

        std::shared_ptr<TSession> GetSessionOptional(const NExport::TIdentifier& id) const {
            auto it = Sessions.find(id);
            if (it == Sessions.end()) {
                return nullptr;
            }
            return it->second;
        }

        std::shared_ptr<TSession> GetSessionVerified(const NExport::TIdentifier& id) const {
            auto result = GetSessionOptional(id);
            AFL_VERIFY(result);
            return result;
        }

        void RemoveSession(const NExport::TIdentifier& id, NTabletFlatExecutor::TTransactionContext& txc);

        bool Load(NTable::TDatabase& database);

    };
}

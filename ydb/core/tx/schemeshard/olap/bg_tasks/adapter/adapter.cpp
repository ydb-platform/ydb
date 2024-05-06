#include "adapter.h"
#include <ydb/core/tx/schemeshard/schemeshard_schema.h>

namespace NKikimr::NSchemeShard::NBackground {

bool TAdapter::DoLoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<NKikimr::NOlap::NBackground::TSessionRecord>& records) {
    NIceDb::TNiceDb db(txc.DB);
    using BackgroundSessions = NSchemeShard::Schema::BackgroundSessions;
    auto rowset = db.Table<BackgroundSessions>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    std::deque<NOlap::NBackground::TSessionRecord> result;
    while (!rowset.EndOfSet()) {
        NOlap::NBackground::TSessionRecord sRecord;
        sRecord.SetClassName(rowset.GetValue<BackgroundSessions::ClassName>());
        sRecord.SetIdentifier(rowset.GetValue<BackgroundSessions::Identifier>());
        sRecord.SetLogicDescription(rowset.GetValue<BackgroundSessions::LogicDescription>());
        sRecord.SetStatusChannel(rowset.GetValue<BackgroundSessions::StatusChannel>());
        sRecord.SetProgress(rowset.GetValue<BackgroundSessions::Progress>());
        sRecord.SetState(rowset.GetValue<BackgroundSessions::State>());
        result.emplace_back(std::move(sRecord));
        if (!rowset.Next()) {
            return false;
        }
    }
    std::swap(result, records);
    return true;
}

void TAdapter::DoSaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) {
    NIceDb::TNiceDb db(txc.DB);
    using BackgroundSessions = NSchemeShard::Schema::BackgroundSessions;
    db.Table<BackgroundSessions>().Key(container.GetClassName(), container.GetIdentifier()).Update(
        NIceDb::TUpdate<BackgroundSessions::Progress>(container.GetProgress())
    );
}

void TAdapter::DoSaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) {
    NIceDb::TNiceDb db(txc.DB);
    using BackgroundSessions = NSchemeShard::Schema::BackgroundSessions;
    db.Table<BackgroundSessions>().Key(container.GetClassName(), container.GetIdentifier()).Update(
        NIceDb::TUpdate<BackgroundSessions::State>(container.GetState())
    );
}

void TAdapter::DoSaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) {
    NIceDb::TNiceDb db(txc.DB);
    using BackgroundSessions = NSchemeShard::Schema::BackgroundSessions;
    db.Table<BackgroundSessions>().Key(container.GetClassName(), container.GetIdentifier()).Update(
        NIceDb::TUpdate<BackgroundSessions::LogicDescription>(container.GetLogicDescription()),
        NIceDb::TUpdate<BackgroundSessions::StatusChannel>(container.GetStatusChannel()),
        NIceDb::TUpdate<BackgroundSessions::Progress>(container.GetProgress()),
        NIceDb::TUpdate<BackgroundSessions::State>(container.GetState())
    );
}

void TAdapter::DoRemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) {
    NIceDb::TNiceDb db(txc.DB);
    using BackgroundSessions = NSchemeShard::Schema::BackgroundSessions;
    db.Table<BackgroundSessions>().Key(className, identifier).Delete();
}

}
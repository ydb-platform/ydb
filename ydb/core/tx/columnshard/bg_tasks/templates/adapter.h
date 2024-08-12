#pragma once
#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>

namespace NKikimr::NTx::NBackground {

template <class Schema>
class TAdapterTemplate: public NKikimr::NOlap::NBackground::ITabletAdapter {
private:
    using TBase = NKikimr::NOlap::NBackground::ITabletAdapter;
protected:
    virtual bool DoLoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<NKikimr::NOlap::NBackground::TSessionRecord>& records) override {
        NIceDb::TNiceDb db(txc.DB);
        using BackgroundSessions = typename Schema::BackgroundSessions;
        auto rowset = db.Table<BackgroundSessions>().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        std::deque<NOlap::NBackground::TSessionRecord> result;
        while (!rowset.EndOfSet()) {
            NOlap::NBackground::TSessionRecord sRecord;
            sRecord.SetClassName(rowset.template GetValue<typename BackgroundSessions::ClassName>());
            sRecord.SetIdentifier(rowset.template GetValue<typename BackgroundSessions::Identifier>());
            sRecord.SetLogicDescription(rowset.template GetValue<typename BackgroundSessions::LogicDescription>());
            sRecord.SetStatusChannel(rowset.template GetValue<typename BackgroundSessions::StatusChannel>());
            sRecord.SetProgress(rowset.template GetValue<typename BackgroundSessions::Progress>());
            sRecord.SetState(rowset.template GetValue<typename BackgroundSessions::State>());
            result.emplace_back(std::move(sRecord));
            if (!rowset.Next()) {
                return false;
            }
        }
        std::swap(result, records);
        return true;
    }

    virtual void DoSaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) override {
        NIceDb::TNiceDb db(txc.DB);
        using BackgroundSessions = typename Schema::BackgroundSessions;
        db.Table<BackgroundSessions>().Key(container.GetClassName(), container.GetIdentifier()).Update(
            NIceDb::TUpdate<typename BackgroundSessions::Progress>(container.GetProgress())
        );
    }

    virtual void DoSaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) override {
        NIceDb::TNiceDb db(txc.DB);
        using BackgroundSessions = typename Schema::BackgroundSessions;
        db.Table<BackgroundSessions>().Key(container.GetClassName(), container.GetIdentifier()).Update(
            NIceDb::TUpdate<typename BackgroundSessions::State>(container.GetState())
        );
    }

    virtual void DoSaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const NKikimr::NOlap::NBackground::TSessionRecord& container) override {
        NIceDb::TNiceDb db(txc.DB);
        using BackgroundSessions = typename Schema::BackgroundSessions;
        db.Table<BackgroundSessions>().Key(container.GetClassName(), container.GetIdentifier()).Update(
            NIceDb::TUpdate<typename BackgroundSessions::LogicDescription>(container.GetLogicDescription()),
            NIceDb::TUpdate<typename BackgroundSessions::StatusChannel>(container.GetStatusChannel()),
            NIceDb::TUpdate<typename BackgroundSessions::Progress>(container.GetProgress()),
            NIceDb::TUpdate<typename BackgroundSessions::State>(container.GetState())
        );
    }

    virtual void DoRemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) override {
        NIceDb::TNiceDb db(txc.DB);
        using BackgroundSessions = typename Schema::BackgroundSessions;
        db.Table<BackgroundSessions>().Key(className, identifier).Delete();
    }

public:
    using TBase::TBase;
};
}
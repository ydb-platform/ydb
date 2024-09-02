#pragma once
#include "accessor_snapshot_base.h"

namespace NKikimr::NMetadata::NProvider {

class TDSAccessorRefresher: public TDSAccessorBase {
private:
    using TBase = TDSAccessorBase;
    YDB_READONLY_DEF(NFetcher::ISnapshot::TPtr, CurrentSnapshot);
    YDB_READONLY_DEF(Ydb::Table::ExecuteQueryResult, CurrentSelection);
    Ydb::Table::ExecuteQueryResult ProposedProto;
    bool FetchingRequestIsRunning = false;
    const TConfig Config;

    void Handle(TEvRefresh::TPtr& ev);

    virtual void OnNewEnrichedSnapshot(NFetcher::ISnapshot::TPtr snapshot) override final;

    virtual void OnNewParsedSnapshot(Ydb::Table::ExecuteQueryResult&& qResult, NFetcher::ISnapshot::TPtr snapshot) override final;

    virtual void OnConstructSnapshotError(const TString& errorMessage) override final;
protected:
    virtual void OnBootstrap() override;
    virtual void OnSnapshotModified() = 0;
    virtual void OnSnapshotRefresh() = 0;
    bool IsReady() const {
        return !!CurrentSnapshot;
    }
public:
    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRefresh, Handle);
            default:
                TBase::StateMain(ev);
        }
    }
    TDSAccessorRefresher(const TConfig& config, NFetcher::ISnapshotsFetcher::TPtr snapshotConstructor)
        : TBase(config.GetRequestConfig(), snapshotConstructor)
        , Config(config)
    {

    }

};

}

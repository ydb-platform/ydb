#pragma once
#include "accessor_init.h"
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/services/metadata/abstract/common.h>

namespace NKikimr::NMetadataProvider {

class TDSAccessorRefresher;

class TEvRefresh: public NActors::TEventLocal<TEvRefresh, EEvSubscribe::EvRefresh> {
public:
};

class TDSAccessorRefresher: public TDSAccessorInitialized {
private:
    using TBase = TDSAccessorInitialized;
    ISnapshotParser::TPtr SnapshotConstructor;
    YDB_READONLY_DEF(ISnapshot::TPtr, CurrentSnapshot);
    YDB_READONLY_DEF(Ydb::ResultSet, CurrentSelection);
    TInstant RequestedActuality = TInstant::Zero();
protected:
    virtual TString GetTableName() const = 0;
    bool IsReady() const {
        return !!CurrentSnapshot;
    }
public:
    using TBase::Handle;
    virtual bool Handle(TEvRequestResult<TDialogCreateTable>::TPtr& ev) override;

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRequestResult<TDialogSelect>, Handle);
            hFunc(TEvRequestResult<TDialogCreateTable>, Handle);
            hFunc(TEvRequestResult<TDialogCreateSession>, Handle);
            hFunc(TEvRefresh, Handle);
            default:
                TBase::StateMain(ev, ctx);
        }
    }

    TDSAccessorRefresher(const TConfig& config, ISnapshotParser::TPtr snapshotConstructor);

    virtual bool Handle(TEvRequestResult<TDialogSelect>::TPtr& ev);
    void Handle(TEvRequestResult<TDialogCreateSession>::TPtr& ev);
    void Handle(TEvRefresh::TPtr& ev);
};

}

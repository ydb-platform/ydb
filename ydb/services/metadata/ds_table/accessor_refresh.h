#pragma once
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/services/metadata/abstract/common.h>
#include <ydb/services/metadata/initializer/accessor_init.h>
#include <ydb/services/metadata/request/request_actor.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NKikimr::NMetadataProvider {

class TDSAccessorRefresher;

class TEvRefresh: public NActors::TEventLocal<TEvRefresh, EEvSubscribe::EvRefresh> {
public:
};

class TEvEnrichSnapshotResult: public NActors::TEventLocal<TEvEnrichSnapshotResult, EEvSubscribe::EvEnrichSnapshot> {
private:
    YDB_READONLY_FLAG(Success, false);
    YDB_READONLY_DEF(TString, ErrorText);
    YDB_READONLY_DEF(ISnapshot::TPtr, EnrichedSnapshot);
public:
    TEvEnrichSnapshotResult(const TString& errorText)
        : ErrorText(errorText) {

    }

    TEvEnrichSnapshotResult(ISnapshot::TPtr snapshot)
        : SuccessFlag(true)
        , EnrichedSnapshot(snapshot)
    {

    }
};

class TDSAccessorRefresher: public TDSAccessorInitialized {
private:
    using TBase = TDSAccessorInitialized;
    ISnapshotParser::TPtr SnapshotConstructor;
    YDB_READONLY_DEF(ISnapshot::TPtr, CurrentSnapshot);
    YDB_READONLY_DEF(Ydb::Table::ExecuteQueryResult, CurrentSelection);
    TInstant RequestedActuality = TInstant::Zero();
protected:
    bool IsReady() const {
        return !!CurrentSnapshot;
    }
    virtual void OnInitialized() override;
    virtual void OnSnapshotModified() = 0;
public:
    using TBase::Handle;

    STFUNC(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogSelect>, Handle);
            hFunc(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogCreateSession>, Handle);
            hFunc(TEvRefresh, Handle);
            hFunc(TEvEnrichSnapshotResult, Handle);
            default:
                TBase::StateMain(ev, ctx);
        }
    }

    TDSAccessorRefresher(const TConfig& config, ISnapshotParser::TPtr snapshotConstructor);

    void Handle(TEvEnrichSnapshotResult::TPtr& ev);
    void Handle(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogSelect>::TPtr& ev);
    void Handle(NInternal::NRequest::TEvRequestResult<NInternal::NRequest::TDialogCreateSession>::TPtr& ev);
    void Handle(TEvRefresh::TPtr& ev);
};

}

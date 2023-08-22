#pragma once

#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/services/metadata/common/ss_dialog.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/manager/abstract.h>

namespace NKikimr::NMetadata::NProvider {

class TTableInfo {
private:
    NSchemeCache::TSchemeCacheNavigate::TEntry Entry;
public:
    TTableInfo(NSchemeCache::TSchemeCacheNavigate::TEntry&& entry)
        : Entry(std::move(entry))
    {

    }

    std::vector<TSysTables::TTableColumnInfo> GetPKFields() const;
    std::vector<TString> GetPKFieldNames() const;

    const NSchemeCache::TSchemeCacheNavigate::TEntry* operator->() const {
        return &Entry;
    }

    NSchemeCache::TSchemeCacheNavigate::TEntry* operator->() {
        return &Entry;
    }
};

class ISchemeDescribeController {
public:
    using TPtr = std::shared_ptr<ISchemeDescribeController>;
    virtual ~ISchemeDescribeController() = default;
    virtual void OnDescriptionFailed(const TString& errorMessage, const TString& requestId) = 0;
    virtual void OnDescriptionSuccess(TTableInfo&& result, const TString& requestId) = 0;
};

class TEvTableDescriptionFailed: public TEventLocal<TEvTableDescriptionFailed, EEvents::EvTableDescriptionFailed> {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_READONLY_DEF(TString, RequestId);
public:
    explicit TEvTableDescriptionFailed(const TString& errorMessage, const TString& reqId)
        : ErrorMessage(errorMessage)
        , RequestId(reqId) {

    }
};

class TEvTableDescriptionSuccess: public TEventLocal<TEvTableDescriptionSuccess, EEvents::EvTableDescriptionSuccess> {
private:
    using TDescription = THashMap<ui32, TSysTables::TTableColumnInfo>;
    YDB_READONLY_DEF(TString, RequestId);
    YDB_READONLY_DEF(TDescription, Description);
public:
    TEvTableDescriptionSuccess(TDescription&& description, const TString& reqId)
        : RequestId(reqId)
        , Description(std::move(description)) {
    }

    NModifications::TTableSchema GetSchema() const {
        return NModifications::TTableSchema(Description);
    }
};

class TSchemeDescriptionActorImpl: public NActors::TActorBootstrapped<TSchemeDescriptionActorImpl> {
private:
    using TBase = NActors::TActorBootstrapped<TSchemeDescriptionActorImpl>;
    ISchemeDescribeController::TPtr Controller;
    TString RequestId;
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev);
protected:
    virtual void InitEntry(NSchemeCache::TSchemeCacheNavigate::TEntry& entry) = 0;
    virtual TString DebugString() const = 0;
public:
    static NKikimrServices::TActivity::EType ActorActivityType();
    void Bootstrap();

    STATEFN(StateMain) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            default:
                return;
        }
    }
    TSchemeDescriptionActorImpl(ISchemeDescribeController::TPtr controller, const TString& reqId)
        : Controller(controller)
        , RequestId(reqId)
    {

    }
};

class TSchemeDescriptionActor: public TSchemeDescriptionActorImpl {
private:
    using TBase = TSchemeDescriptionActorImpl;
    TString Path;
protected:
    virtual void InitEntry(NSchemeCache::TSchemeCacheNavigate::TEntry& entry) override {
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
        entry.Path = NKikimr::SplitPath(Path);
    }
    virtual TString DebugString() const override {
        return "PATH:" + Path;
    }
public:
    TSchemeDescriptionActor(ISchemeDescribeController::TPtr controller, const TString& reqId, const TString& path)
        : TBase(controller, reqId)
        , Path(path) {

    }
};

class TSchemeDescriptionActorByTableId: public TSchemeDescriptionActorImpl {
private:
    using TBase = TSchemeDescriptionActorImpl;
    const TTableId TableId;
    virtual TString DebugString() const override {
        return "TableId:" + TableId.PathId.ToString();
    }
protected:
    virtual void InitEntry(NSchemeCache::TSchemeCacheNavigate::TEntry& entry) override {
        entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByTableId;
        entry.TableId = TableId;
    }
public:
    TSchemeDescriptionActorByTableId(ISchemeDescribeController::TPtr controller, const TString& reqId, const TTableId& tableId)
        : TBase(controller, reqId)
        , TableId(tableId) {

    }
};

}

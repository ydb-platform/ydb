#pragma once
#include "object.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/tiering/common.h>

#include <ydb/services/bg_tasks/abstract/interface.h>

namespace NKikimr::NColumnShard::NTiers {

class TFetcherCheckUserTieringPermissionsResult: public NBackgroundTasks::IProtoStringSerializable<
    NKikimrScheme::TFetcherCheckUserTieringPermissionsResult, NBackgroundTasks::IStringSerializable> {
private:
    using TProtoClass = NKikimrScheme::TFetcherCheckUserTieringPermissionsResult;
    YDB_ACCESSOR_DEF(TProtoClass, Content);
protected:
    virtual TProtoClass DoSerializeToProto() const override {
        return Content;
    }
    virtual bool DoDeserializeFromProto(const TProtoClass& protoData) override {
        Content = protoData;
        return true;
    }
public:
    void Deny(const TString& reason) {
        Content.SetOperationAllow(false);
        Content.SetDenyReason(reason);
    }
};

class TFetcherCheckUserTieringPermissions: public NBackgroundTasks::IProtoStringSerializable<
    NKikimrScheme::TFetcherCheckUserTieringPermissions, NSchemeShard::ISSDataProcessor> {
private:
    using TBase = NBackgroundTasks::IProtoStringSerializable<
        NKikimrScheme::TFetcherCheckUserTieringPermissions, NSchemeShard::ISSDataProcessor>;
    using TBase::TFactory;
    using TProtoClass = NKikimrScheme::TFetcherCheckUserTieringPermissions;
    static TFactory::TRegistrator<TFetcherCheckUserTieringPermissions> Registrator;
    YDB_ACCESSOR_DEF(std::set<TString>, TieringRuleIds);
    YDB_ACCESSOR_DEF(std::optional<NACLib::TUserToken>, UserToken);
    YDB_ACCESSOR(NMetadata::NModifications::IOperationsManager::EActivityType, ActivityType,
        NMetadata::NModifications::IOperationsManager::EActivityType::Undefined);
protected:
    virtual TProtoClass DoSerializeToProto() const override;
    virtual bool DoDeserializeFromProto(const TProtoClass& protoData) override;
    virtual void DoProcess(NSchemeShard::TSchemeShard& schemeShard, NKikimrScheme::TEvProcessingResponse& result) const override;
public:
    using TResult = TFetcherCheckUserTieringPermissionsResult;
    std::optional<TFetcherCheckUserTieringPermissionsResult> UnpackResult(const TString& content) const {
        TFetcherCheckUserTieringPermissionsResult result;
        if (!result.DeserializeFromString(content)) {
            return {};
        } else {
            return result;
        }
    }

    TFetcherCheckUserTieringPermissions() = default;

    virtual TString DebugString() const override {
        TStringBuilder sb;
        sb << "USID=" << (UserToken ? UserToken->GetUserSID() : "nobody") << ";";
        sb << "tierings=";
        for (auto&& i : TieringRuleIds) {
            sb << i << ",";
        }
        sb << ";";
        return sb;
    }
    virtual TString GetClassName() const override {
        return GetTypeIdStatic();
    }
    static TString GetTypeIdStatic() {
        return "ss_fetcher_tiering_permissions";
    }
};

}

#pragma once
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/service.h>

namespace NKikimr::NMetadata::NModifications {

class IBuildRequestController {
public:
    using TPtr = std::shared_ptr<IBuildRequestController>;

    virtual void OnBuildFinished(NKikimrSchemeOp::TModifyScheme request, std::optional<NACLib::TUserToken> userToken) = 0;
    virtual void OnBuildProblem(const TString& errorMessage) = 0;

    virtual ~IBuildRequestController() = default;
};

class TSchemeObjectOperationsManager: public IOperationsManager {
private:
    using IOperationsManager::TYqlConclusionStatus;

public:
    using TPtr = std::shared_ptr<TSchemeObjectOperationsManager>;

protected:
    using TInternalModificationContext = typename IOperationsManager::TInternalModificationContext;
    using TExternalModificationContext = typename IOperationsManager::TExternalModificationContext;

    using IBuildRequestController = IBuildRequestController;

    virtual void DoBuildRequestFromSettings(
        const NYql::TObjectSettingsImpl& settings, TInternalModificationContext& context, IBuildRequestController::TPtr controller) const = 0;

protected:
    NThreading::TFuture<TYqlConclusionStatus> DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 /*nodeId*/,
        const IClassBehaviour::TPtr& manager, TInternalModificationContext& context) const override;

    TYqlConclusionStatus DoPrepare(NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const NYql::TObjectSettingsImpl& settings,
        const IClassBehaviour::TPtr& /*manager*/, TInternalModificationContext& /*context*/) const override {
        return TYqlConclusionStatus::Fail("Prepare operation is not supported for " + settings.GetTypeId() + " objects.");
    }

    NThreading::TFuture<TYqlConclusionStatus> ExecutePrepared(const NKqpProto::TKqpSchemeOperation& /*schemeOperation*/, const ui32 /*nodeId*/,
        const IClassBehaviour::TPtr& /*manager*/, const TExternalModificationContext& /*context*/) const override {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail("Execute prepared operation is not supported for this object."));
    }

    static NKikimrSchemeOp::TModifyACL MakeModifyACL(const TString& objectId, const std::optional<NACLib::TUserToken>& owner) {
        NACLib::TDiffACL diffAcl;
        for (const TString& usedSid : AppData()->AdministrationAllowedSIDs) {
            diffAcl.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::GenericFull, usedSid);
        }

        auto useAccess = NACLib::EAccessRights::SelectRow | NACLib::EAccessRights::DescribeSchema;
        for (const auto& userSID : AppData()->DefaultUserSIDs) {
            diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, userSID);
        }
        diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, AppData()->AllAuthenticatedUsers);
        diffAcl.AddAccess(NACLib::EAccessType::Allow, useAccess, BUILTIN_ACL_ROOT);

        auto token = MakeIntrusive<NACLib::TUserToken>(BUILTIN_ACL_METADATA, TVector<NACLib::TSID>{});
        ::NKikimrSchemeOp::TModifyACL modifyACL;

        modifyACL.SetName(objectId);
        modifyACL.SetDiffACL(diffAcl.SerializeAsString());
        if (owner) {
            modifyACL.SetNewOwner(owner->GetUserSID());
        }

        return modifyACL;
    }
};

}   // namespace NKikimr::NMetadata::NModifications

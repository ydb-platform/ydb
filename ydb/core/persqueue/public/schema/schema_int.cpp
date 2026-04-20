#include "schema_int.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

namespace NKikimr::NPQ::NSchema {

namespace {

TString GetWorkingDir(const NDescriber::TTopicInfo& topicInfo) {
    std::pair <TString, TString> pathPair;
    try {
        pathPair = NKikimr::NGRpcService::SplitPath(topicInfo.RealPath);
    } catch (const std::exception &ex) {
        return {};
    }
    return pathPair.first;
}
        
} // namespace
        
        
//
// IAlterTopicStrategy
//
bool IAlterTopicStrategy::IsCdcStreamCompatible() const {
    return true;
}

NACLib::EAccessRights IAlterTopicStrategy::GetRequiredPermission() const {
    return NACLib::EAccessRights::AlterSchema;
}

TResult IAlterTopicStrategy::BuildTransaction(
    const NDescriber::TTopicInfo& topicInfo, 
    NKikimrTxUserProxy::TTransaction* transaction
) {
    NKikimrSchemeOp::TModifyScheme& modifyScheme = *transaction->MutableModifyScheme();

    auto workingDir = GetWorkingDir(topicInfo);
    if (workingDir.empty()) {
        return {Ydb::StatusIds::SCHEME_ERROR, "Wrong topic name"};
    }

    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpAlterPersQueueGroup);
    modifyScheme.SetWorkingDir(workingDir);
    modifyScheme.SetAllowAccessToPrivatePaths(true);

    auto* config = modifyScheme.MutableAlterPersQueueGroup();
    config->CopyFrom(topicInfo.Info->Description);

    // keep previous values or set in ModifyPersqueueConfig
    config->ClearTotalGroupCount();
    config->MutablePQTabletConfig()->ClearPartitionKeySchema();

    {
        auto applyIf = modifyScheme.AddApplyIf();
        applyIf->SetPathId(topicInfo.Self->Info.GetPathId());
        applyIf->SetPathVersion(topicInfo.Self->Info.GetPathVersion());
    }

    auto result = ApplyChanges(*config, topicInfo.Info->Description, topicInfo.CdcStream);

    ModifyScheme = modifyScheme;

    return result;
}

IEventBase* IAlterTopicStrategy::CreateSuccessResponse() {
    return new TEvAlterTopicResponse();
}

IEventBase* IAlterTopicStrategy::CreateErrorResponse(Ydb::StatusIds::StatusCode status, TString&& errorMessage) {
    return new TEvAlterTopicResponse(status, std::move(errorMessage), std::move(ModifyScheme));
}

//
// IDropTopicStrategy
//
bool IDropTopicStrategy::IsCdcStreamCompatible() const {
    return false;
}

NACLib::EAccessRights IDropTopicStrategy::GetRequiredPermission() const {
    return NACLib::EAccessRights::RemoveSchema;
}

TResult IDropTopicStrategy::BuildTransaction(
    const NDescriber::TTopicInfo& topicInfo, 
    NKikimrTxUserProxy::TTransaction* transaction
) {
    NKikimrSchemeOp::TModifyScheme& modifyScheme = *transaction->MutableModifyScheme();

    auto workingDir = GetWorkingDir(topicInfo);
    if (workingDir.empty()) {
        return {Ydb::StatusIds::SCHEME_ERROR, "Wrong topic name"};
    }

    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropPersQueueGroup);
    modifyScheme.SetWorkingDir(workingDir);
    modifyScheme.SetAllowAccessToPrivatePaths(false);

    auto* config = modifyScheme.MutableDrop();
    config->SetName(topicInfo.Info->Description.name());

    return {};
}

IEventBase* IDropTopicStrategy::CreateSuccessResponse() {
    return new TEvDropTopicResponse();
}

IEventBase* IDropTopicStrategy::CreateErrorResponse(Ydb::StatusIds::StatusCode status, TString&& errorMessage) {
    return new TEvDropTopicResponse(status, std::move(errorMessage));
}

} // namespace NKikimr::NPQ::NSchema

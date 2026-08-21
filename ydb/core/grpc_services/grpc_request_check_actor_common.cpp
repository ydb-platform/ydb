#include "grpc_request_check_actor.h"

#include <ydb/core/audit/audit_config/audit_config.h>
#include <ydb/core/security/secure_request_iface.h>

namespace NKikimr::NGRpcService {

void TGrpcRequestCheckActorCommon::AuditRequest(IRequestProxyCtx* requestBaseCtx, const TString& databaseName, bool isGrpcRequest, const TString& userSID, bool auditEnabledCompleted, const ISecureRequestIface& secReq) const {
    bool auditEnabledReceived = false;

    TAuditMode auditMode = requestBaseCtx->GetAuditMode();
    if (auditMode.IsModifying && !requestBaseCtx->IsInternalCall()) {
        TIntrusiveConstPtr<NACLib::TUserToken> token = secReq.GetParsedToken();
        const NACLibProto::ESubjectType subjectType = token ? token->GetSubjectType() : NACLibProto::SUBJECT_TYPE_ANONYMOUS;
        auditEnabledCompleted |= AppData()->AuditConfig.EnableLogging(auditMode.LogClass, NKikimrConfig::TAuditConfig::TLogClassConfig::Completed, subjectType);
        auditEnabledReceived |= AppData()->AuditConfig.EnableLogging(auditMode.LogClass, NKikimrConfig::TAuditConfig::TLogClassConfig::Received, subjectType);
    }

    if (auditEnabledReceived || auditEnabledCompleted) {
        if (isGrpcRequest) {
            if (TString grpcMethod = requestBaseCtx->GetRpcMethodName()) {
                requestBaseCtx->AddAuditLogPart("grpc_method", requestBaseCtx->GetRpcMethodName());
            }
        }
        const TString sanitizedToken = secReq.GetSanitizedToken();
        AuditContextStart(requestBaseCtx, databaseName, userSID, sanitizedToken, Attributes_);
        if (auditEnabledReceived) {
            AuditLog(std::nullopt, requestBaseCtx->GetAuditLogParts());
        }

        if (auditEnabledCompleted) {
            requestBaseCtx->SetAuditLogHook([requestBaseCtx](ui32 status, const TAuditLogParts& parts) {
                AuditContextEnd(requestBaseCtx);
                AuditLog(status, parts);
            });
        }
    }
}

}

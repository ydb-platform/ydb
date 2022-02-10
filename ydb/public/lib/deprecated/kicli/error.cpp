#include "kicli.h"
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/public/lib/base/defs.h>
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/protos/minikql_engine.pb.h>

namespace NKikimr {
namespace NClient {

bool TError::Success() const {
    switch(Facility) {
    case EFacility::FacilityMessageBus:
        switch (Code) {
        case NBus::MESSAGE_OK:
            return true;
        default:
            break;
        };
        break;
    case EFacility::FacilityMsgBusProxy:
        switch (Code) {
        case NMsgBusProxy::MSTATUS_INPROGRESS:
        case NMsgBusProxy::MSTATUS_OK:
            return true;
        default:
            break;
        };
        break;
    case EFacility::FacilityExecutionEngine:
        switch (Code) {
        case NKikimrMiniKQLEngine::Ok:
            return true;
        default:
            break;
        };
        break;
    case EFacility::FacilityTxProxy:
        switch (Code) {
        case NTxProxy::TResultStatus::EStatus::ExecComplete:
        case NTxProxy::TResultStatus::EStatus::ExecAlready:
        case NTxProxy::TResultStatus::EStatus::ExecInProgress:
        case NTxProxy::TResultStatus::EStatus::ExecResponseData:
            return true;
        default:
            break;
        };
        break;
    };
    return false;
}

bool TError::Permanent() const {
    switch(Facility) {
    case EFacility::FacilityMessageBus:
        switch (Code) {
        case NBus::MESSAGE_BUSY:
        case NBus::MESSAGE_TIMEOUT:
            return false;
        default:
            break;
        };
        break;
    case EFacility::FacilityMsgBusProxy:
        switch (Code) {
        case NMsgBusProxy::MSTATUS_INPROGRESS:
        case NMsgBusProxy::MSTATUS_NOTREADY:
        case NMsgBusProxy::MSTATUS_TIMEOUT:
        case NMsgBusProxy::MSTATUS_REJECTED:
            return false;
        default:
            break;
        };
        break;
    case EFacility::FacilityExecutionEngine:
        switch (Code) {
        case NKikimrMiniKQLEngine::SnapshotNotReady:
            return false;
        default:
            break;
        };
        break;
    case EFacility::FacilityTxProxy:
        switch (Code) {
        case NTxProxy::TResultStatus::EStatus::ExecTimeout:
        case NTxProxy::TResultStatus::EStatus::ExecInProgress:
        case NTxProxy::TResultStatus::EStatus::ProxyShardTryLater:
        case NTxProxy::TResultStatus::EStatus::ProxyShardOverloaded:
        case NTxProxy::TResultStatus::EStatus::ProxyShardNotAvailable:
            return false;
        default:
            break;
        };
        break;
    };
    return Error();
}

bool TError::Timeout() const {
    switch(Facility) {
    case EFacility::FacilityMessageBus:
        switch (Code) {
        case NBus::MESSAGE_TIMEOUT:
            return true;
        default:
            break;
        };
        break;
    case EFacility::FacilityMsgBusProxy:
        switch (Code) {
        case NMsgBusProxy::MSTATUS_TIMEOUT:
            return true;
        default:
            break;
        };
        break;
    case EFacility::FacilityExecutionEngine:
        break;
    case EFacility::FacilityTxProxy:
        switch (Code) {
        case NTxProxy::TResultStatus::EStatus::ExecTimeout:
            return true;
        default:
            break;
        };
        break;
    };
    return false;
}

bool TError::Rejected() const {
    switch(Facility) {
    case EFacility::FacilityMessageBus:
        switch (Code) {
        case NBus::MESSAGE_BUSY:
            return true;
        default:
            break;
        };
        break;
    case EFacility::FacilityMsgBusProxy:
        switch (Code) {
        case NMsgBusProxy::MSTATUS_NOTREADY:
        case NMsgBusProxy::MSTATUS_REJECTED:
            return true;
        default:
            break;
        };
        break;
    case EFacility::FacilityExecutionEngine:
        break;
    case EFacility::FacilityTxProxy:
        switch (Code) {
        case NTxProxy::TResultStatus::EStatus::ProxyShardTryLater:
        case NTxProxy::TResultStatus::EStatus::ProxyShardOverloaded:
        case NTxProxy::TResultStatus::EStatus::ProxyShardNotAvailable:
        case NTxProxy::TResultStatus::EStatus::CoordinatorDeclined:
        case NTxProxy::TResultStatus::EStatus::CoordinatorAborted:
        case NTxProxy::TResultStatus::EStatus::CoordinatorOutdated:
            return true;
        default:
            break;
        };
        break;
    };
    return false;
}

TString TError::GetCode() const {
    switch(Facility) {
    case EFacility::FacilityMessageBus:
        return Sprintf("MB-%04" PRIu16, Code);
    case EFacility::FacilityExecutionEngine:
        return Sprintf("EE-%04" PRIu16, Code);
    case EFacility::FacilityTxProxy:
        return Sprintf("TX-%04" PRIu16, Code);
    case EFacility::FacilityMsgBusProxy:
        return Sprintf("MP-%04" PRIu16, Code);
    }
    return TString();
}

TString TError::GetMessage() const {
    if (!Message.empty())
        return Message;
    switch(Facility) {
    case EFacility::FacilityMessageBus:
        return NBus::MessageStatusDescription((NBus::EMessageStatus)Code);
    case EFacility::FacilityExecutionEngine:
        switch (Code) {
        case NKikimrMiniKQLEngine::Unknown:
            return "Status unknown, not filled probably";
        case NKikimrMiniKQLEngine::Ok:
            return "Success";
        case NKikimrMiniKQLEngine::SchemeChanged:
            return "Scheme or partitioning was changed b/w compilation and preparation";
        case NKikimrMiniKQLEngine::IsReadonly:
            return "Update requested in read-only operation";
        case NKikimrMiniKQLEngine::KeyError:
            return "Something wrong in data keys";
        case NKikimrMiniKQLEngine::ProgramError:
            return "Malformed program";
        case NKikimrMiniKQLEngine::TooManyShards:
            return "Too many datashards affected by program";
        case NKikimrMiniKQLEngine::TooManyData:
            return "Too much data affected by program";
        case NKikimrMiniKQLEngine::SnapshotNotExist:
            return "Requested snapshot not exist";
        case NKikimrMiniKQLEngine::SnapshotNotReady:
            return "Snapshot not online";
        case NKikimrMiniKQLEngine::TooManyRS:
            return "Too many data transfers between datashards in the program";
        }
        break;
    case EFacility::FacilityTxProxy:
        switch (Code) {
        case NTxProxy::TResultStatus::EStatus::Unknown:
            return "Status unknown, must not be seen";
        case NTxProxy::TResultStatus::EStatus::WrongRequest:
            return "Not recognized or erroneous request, see error description fields for possible details";
        case NTxProxy::TResultStatus::EStatus::EmptyAffectedSet:
            return "Program must touch at least one shard but touches none";
        case NTxProxy::TResultStatus::EStatus::NotImplemented:
            return "Not yet implemented feature requested";
        case NTxProxy::TResultStatus::EStatus::ResolveError:
            return "Some keys not resolved, see UnresolvedKeys for details";
        case NTxProxy::TResultStatus::EStatus::AccessDenied:
            return "Access denied";
        case NTxProxy::TResultStatus::EStatus::ProxyNotReady:
            return "Transaction proxy not ready for handling requests, try later. Most known case is temporary lack of txid-s";
        case NTxProxy::TResultStatus::EStatus::ProxyAccepted:
            return "Request accepted by proxy. Transitional status";
        case NTxProxy::TResultStatus::EStatus::ProxyResolved:
            return "Request keys resolved to datashards. Transitional status";
        case NTxProxy::TResultStatus::EStatus::ProxyPrepared:
            return "Request fragmets prepared on datashards. Transitional status";
        case NTxProxy::TResultStatus::EStatus::ProxyShardNotAvailable:
            return "One or more of affected datashards not available, request execution cancelled";
        case NTxProxy::TResultStatus::EStatus::ProxyShardTryLater:
            return "One or more of affected datashards are starting, try again";
        case NTxProxy::TResultStatus::EStatus::ProxyShardOverloaded:
            return "One or more of affected datashards are overloaded, try again";
        case NTxProxy::TResultStatus::EStatus::CoordinatorDeclined:
            return "Coordinator declines to plan transaction, try again";
        case NTxProxy::TResultStatus::EStatus::CoordinatorOutdated:
            return "Coordinator was not able to plan transaction due to timing restrictions, try again";
        case NTxProxy::TResultStatus::EStatus::CoordinatorAborted:
            return "Transaction aborted by coordinator";
        case NTxProxy::TResultStatus::EStatus::CoordinatorPlanned:
            return "Transaction planned for execution by coordinator. Transitional status";
        case NTxProxy::TResultStatus::EStatus::CoordinatorUnknown:
            return "Could not reach coordinator or coordinator pipe dropped before confirmation. Transaction status unknown";
        case NTxProxy::TResultStatus::EStatus::ExecComplete:
            return "Success";
        case NTxProxy::TResultStatus::EStatus::ExecAlready:
            return "Requested operation already applied";
        case NTxProxy::TResultStatus::EStatus::ExecAborted:
            return "Request aborted, particular meaning depends on context";
        case NTxProxy::TResultStatus::EStatus::ExecTimeout:
            return "Proxy got no execution reply in timeout period";
        case NTxProxy::TResultStatus::EStatus::ExecError:
            return "Execution failed";
        case NTxProxy::TResultStatus::EStatus::ExecInProgress:
            return "Request accepted and now runs";
        case NTxProxy::TResultStatus::EStatus::ExecResultUnavailable:
            return "Execution result unavailable.";
        };
        break;
    case EFacility::FacilityMsgBusProxy:
        return NMsgBusProxy::ToCString((NMsgBusProxy::EResponseStatus)Code);
    }
    return "";
}

void TError::Throw() const {
    if (Error())
        throw yexception() << GetCode() << " " << GetMessage();
}

TError::TError(const TResult& result)
    : Facility(EFacility::FacilityMessageBus)
    , Code(NBus::MESSAGE_OK)
    , YdbStatus(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)
{
    if (result.TransportStatus != NBus::MESSAGE_OK) {
        Facility = EFacility::FacilityMessageBus;
        Code = result.TransportStatus;
        Message = result.TransportErrorMessage;
    } else {
        if (result.Reply->GetHeader()->Type == NMsgBusProxy::MTYPE_CLIENT_RESPONSE) {
            const NKikimrClient::TResponse& response = result.GetResult<NKikimrClient::TResponse>();
            if (response.HasExecutionEngineStatus()
                    && response.GetExecutionEngineStatus() != NKikimrMiniKQLEngine::Ok
                    && response.GetExecutionEngineStatus() != NKikimrMiniKQLEngine::Unknown) {
                Facility = EFacility::FacilityExecutionEngine;
                Code = response.GetExecutionEngineStatus();
            } else
            if (response.HasProxyErrorCode()
                    && (NTxProxy::TResultStatus::EStatus)response.GetProxyErrorCode() != NTxProxy::TResultStatus::EStatus::ExecComplete
                    && (NTxProxy::TResultStatus::EStatus)response.GetProxyErrorCode() != NTxProxy::TResultStatus::EStatus::Unknown) {
                Facility = EFacility::FacilityTxProxy;
                Code = response.GetProxyErrorCode();
            } else
            if (response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
                Facility = EFacility::FacilityMsgBusProxy;
                Code = response.GetStatus();
            }
            if (response.HasErrorReason()) {
                Message = response.GetErrorReason();
            }
            if (response.HasDataShardErrors()) {
                Message = response.GetDataShardErrors();
            }
            if (response.HasMiniKQLCompileResults() && response.GetMiniKQLCompileResults().ProgramCompileErrorsSize() > 0) {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(response.GetMiniKQLCompileResults().GetProgramCompileErrors(), issues);
                TStringStream message;
                issues.PrintTo(message);
                Message = message.Str();
            }
            if (response.HasMiniKQLErrors()) {
                if (!Message.empty())
                    Message += '\n';
                Message += response.GetMiniKQLErrors();
            }
        }
    }
}

TError::EFacility TError::GetFacility() const {
    return Facility;
}

Ydb::StatusIds::StatusCode TError::GetYdbStatus() const {
    return YdbStatus;
}

}
}

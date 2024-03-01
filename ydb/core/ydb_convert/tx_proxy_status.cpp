#include "tx_proxy_status.h"

#include <ydb/public/api/protos/ydb_issue_message.pb.h>

namespace NKikimr {


bool IsTxProxyInProgress(const Ydb::StatusIds::StatusCode& code) {
    return code == Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
}

Ydb::StatusIds::StatusCode YdbStatusFromProxyStatus(TEvTxUserProxy::TEvProposeTransactionStatus* msg)
{
    const auto status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
    auto issueMessage = msg->Record.GetIssues();

    switch (status) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete: {
            if (msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusSuccess || msg->Record.GetSchemeShardStatus() == NKikimrScheme::EStatus::StatusAlreadyExists) {
                return Ydb::StatusIds::SUCCESS;
            }
            break;
        }
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress: {
            return Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        }
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest: {
            return Ydb::StatusIds::BAD_REQUEST;
        }
        case TEvTxUserProxy::TResultStatus::AccessDenied: {
            return Ydb::StatusIds::UNAUTHORIZED;
        }
        case TEvTxUserProxy::TResultStatus::ProxyShardNotAvailable: {
            return Ydb::StatusIds::UNAVAILABLE;
        }
        case TEvTxUserProxy::TResultStatus::ResolveError: {
            return Ydb::StatusIds::SCHEME_ERROR;
        }
        case TEvTxUserProxy::TResultStatus::ExecError: {
            switch (msg->Record.GetSchemeShardStatus()) {
            case NKikimrScheme::EStatus::StatusMultipleModifications: {
                return Ydb::StatusIds::OVERLOADED;
            }
            case NKikimrScheme::EStatus::StatusInvalidParameter: {
                return Ydb::StatusIds::BAD_REQUEST;
            }
            case NKikimrScheme::EStatus::StatusSchemeError:
            case NKikimrScheme::EStatus::StatusNameConflict:
            case NKikimrScheme::EStatus::StatusPathDoesNotExist: {
                return Ydb::StatusIds::SCHEME_ERROR;
            }
            case NKikimrScheme::EStatus::StatusQuotaExceeded: {
                return Ydb::StatusIds::OVERLOADED;
            }
            case NKikimrScheme::EStatus::StatusResourceExhausted:
            case NKikimrScheme::EStatus::StatusPreconditionFailed: {
                return Ydb::StatusIds::PRECONDITION_FAILED;
            }
            default: {
                return Ydb::StatusIds::GENERIC_ERROR;
            } 
        }
        }
        default: {
            TStringStream str;
            str << "Got unknown TEvProposeTransactionStatus (" << status << ") response from TxProxy";
            const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, str.Str());
            auto tmp = issueMessage.Add();
            NYql::IssueToMessage(issue, tmp);
            return Ydb::StatusIds::INTERNAL_ERROR;
        }
    }

    return Ydb::StatusIds::INTERNAL_ERROR;
}

}  // namespace NKikimr

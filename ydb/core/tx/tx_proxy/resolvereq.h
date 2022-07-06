#pragma once
#include "proxy.h"

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/yql_issue_manager.h>

namespace NKikimr {
namespace NTxProxy {

    struct TResolveTableRequest {
        TString TablePath;
        NKikimrTxUserProxy::TKeyRange KeyRange;
    };

    struct TResolveTableResponse {
        TString TablePath;
        NKikimrTxUserProxy::TKeyRange KeyRange;
        TTableId TableId;
        TSerializedCellVec FromValues;
        TSerializedCellVec ToValues;
        THolder<TKeyDesc> KeyDescription;
        NSchemeCache::TDomainInfo::TPtr DomainInfo;
        bool IsColumnTable = false;
    };

    using TResolveTableResponses = TVector<TResolveTableResponse>;

    struct TEvResolveTablesResponse : public TEventLocal<TEvResolveTablesResponse, TEvTxUserProxy::EvResolveTablesResponse> {
        TEvTxUserProxy::TEvProposeTransactionStatus::EStatus Status;
        NKikimrIssues::TStatusIds::EStatusCode StatusCode;

        TInstant WallClockResolveStarted;
        TInstant WallClockResolved;

        TResolveTableResponses Tables;

        TVector<TString> UnresolvedKeys;
        NYql::TIssues Issues;

        TEvResolveTablesResponse(
                TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status,
                NKikimrIssues::TStatusIds::EStatusCode statusCode)
            : Status(status)
            , StatusCode(statusCode)
        { }

        bool CheckDomainLocality() const;

        NSchemeCache::TDomainInfo::TPtr FindDomainInfo() const;
    };

    IActor* CreateResolveTablesActor(
            TActorId owner,
            ui64 txId,
            const TTxProxyServices& services,
            TVector<TResolveTableRequest> tables,
            const TString& databaseName);

} // namespace NTxProxy
} // namespace NKikimr

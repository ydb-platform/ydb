#pragma once
#include "defs.h"

#include "rpc_deferrable.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

namespace NKikimr {
namespace NGRpcService {

inline void SetAuthToken(NKikimrKqp::TEvQueryRequest& req, const IRequestCtxMtSafe& ctx) {
    if (ctx.GetSerializedToken()) {
        req.SetUserToken(ctx.GetSerializedToken());
    }
}

inline void SetDatabase(NKikimrKqp::TEvQueryRequest& req, const IRequestCtxMtSafe& ctx) {
    // Empty database in case of absent header
    req.MutableRequest()->SetDatabase(CanonizePath(ctx.GetDatabaseName().GetOrElse("")));
}

inline TString DecodePreparedQueryId(const TString& in) {
    if (in.empty()) {
        throw NYql::TErrorException(NKikimrIssues::TIssuesIds::DEFAULT_ERROR)
            << "got empty preparedQueryId message";
    }
    TString decodedStr;
    bool decoded = NOperationId::DecodePreparedQueryIdCompat(in, decodedStr);
    if (decoded) {
        return decodedStr;
    } else {
        // Do not wrap query id GUID in to operation in the next stable
        return in;
    }
}

inline TString GetTransactionModeName(const Ydb::Table::TransactionSettings& settings) {
    switch (settings.tx_mode_case()) {
        case Ydb::Table::TransactionSettings::kSerializableReadWrite:
            return "SerializableReadWrite";
        case Ydb::Table::TransactionSettings::kOnlineReadOnly:
            return "OnlineReadOnly";
        case Ydb::Table::TransactionSettings::kStaleReadOnly:
            return "StaleReadOnly";
        case Ydb::Table::TransactionSettings::kSnapshotReadOnly:
            return "SnapshotReadOnly";
        default:
            return "Unknown";
    }
}

inline NYql::NDqProto::EDqStatsMode GetKqpStatsMode(Ydb::Table::QueryStatsCollection::Mode mode) {
    switch (mode) {
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC:
            return NYql::NDqProto::DQ_STATS_MODE_BASIC;
        case Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL:
            return NYql::NDqProto::DQ_STATS_MODE_PROFILE;
        default:
            return NYql::NDqProto::DQ_STATS_MODE_NONE;
    }
}

inline bool CheckQuery(const TString& query, NYql::TIssues& issues) {
    if (query.empty()) {
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, "Empty query text"));
        return false;
    }

    return true;
}

void FillQueryStats(Ydb::TableStats::QueryStats& queryStats, const NKqpProto::TKqpStatsQuery& kqpStats);
void FillQueryStats(Ydb::TableStats::QueryStats& queryStats, const NKikimrKqp::TQueryResponse& kqpResponse);

Ydb::Table::QueryStatsCollection::Mode GetCollectStatsMode(Ydb::Query::StatsMode mode);

template <typename TDerived, typename TRequest>
class TRpcKqpRequestActor : public TRpcOperationRequestActor<TDerived, TRequest> {
    using TBase = TRpcOperationRequestActor<TDerived, TRequest>;

public:
    TRpcKqpRequestActor(IRequestOpCtx* request)
        : TBase(request) {}

    void OnOperationTimeout(const TActorContext& ctx) {
        Y_UNUSED(ctx);
    }

protected:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            default: TBase::StateFuncBase(ev);
        }
    }

    template<typename TKqpResponse>
    void AddServerHintsIfAny(const TKqpResponse& kqpResponse) {
        if (kqpResponse.GetWorkerIsClosing()) {
            this->Request_->AddServerHint(TString(NYdb::YDB_SESSION_CLOSE));
        }
    }

    template<typename TKqpResponse>
    void OnGenericQueryResponseError(const TKqpResponse& kqpResponse, const TActorContext& ctx) {
        RaiseIssuesFromKqp(kqpResponse);

        this->Request_->ReplyWithYdbStatus(kqpResponse.GetYdbStatus());
        this->Die(ctx);
    }

    template<typename TKqpResponse>
    void OnQueryResponseErrorWithTxMeta(const TKqpResponse& kqpResponse, const TActorContext& ctx) {
        RaiseIssuesFromKqp(kqpResponse);

        auto queryResult = TRequest::template AllocateResult<typename TDerived::TResult>(this->Request_);
        if (kqpResponse.GetResponse().HasTxMeta()) {
            queryResult->mutable_tx_meta()->CopyFrom(kqpResponse.GetResponse().GetTxMeta());
        }

        this->Request_->SendResult(*queryResult, kqpResponse.GetYdbStatus());
        this->Die(ctx);
    }

    void OnQueryResponseError(const NKikimrKqp::TEvCreateSessionResponse& kqpResponse, const TActorContext& ctx) {
        if (kqpResponse.HasError()) {
            NYql::TIssues issues;
            issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, kqpResponse.GetError()));
            return this->Reply(kqpResponse.GetYdbStatus(), issues, ctx);
        } else {
            return this->Reply(kqpResponse.GetYdbStatus(), ctx);
        }
    }

    template<typename TKqpResponse>
    void OnKqpError(const TKqpResponse& response, const TActorContext& ctx) {
        NYql::TIssues issues;
        NYql::IssuesFromMessage(response.GetIssues(), issues);

        this->Request_->RaiseIssues(issues);
        this->Request_->ReplyWithYdbStatus(response.GetStatus());
        this->Die(ctx);
    }

private:
    template<typename TKqpResponse>
    void RaiseIssuesFromKqp(const TKqpResponse& kqpResponse) {
        NYql::TIssues issues;
        const auto& issueMessage = kqpResponse.GetResponse().GetQueryIssues();
        NYql::IssuesFromMessage(issueMessage, issues);
        this->Request_->RaiseIssues(issues);
    }
};

} // namespace NGRpcService
} // namespace NKikimr

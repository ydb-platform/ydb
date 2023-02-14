#include "kqp.h"

#include <ydb/core/base/path.h>
#include <ydb/core/util/proto_duration.h>

namespace NKikimr::NKqp {

void TEvKqp::TEvQueryRequest::PrepareRemote() const {
    if (RequestCtx) {
        if (RequestCtx->GetInternalToken()) {
            Record.SetUserToken(RequestCtx->GetInternalToken());
        }

        Record.MutableRequest()->SetDatabase(
            CanonizePath(RequestCtx->GetDatabaseName().GetOrElse("")));

        if (auto traceId = RequestCtx->GetTraceId()) {
            Record.SetTraceId(traceId.GetRef());
        }

        if (auto requestType = RequestCtx->GetRequestType()) {
            Record.SetRequestType(requestType.GetRef());
        }

        if (TxControl) {
            Record.MutableRequest()->MutableTxControl()->CopyFrom(*TxControl);
        }

        if (YdbParameters) {
            Record.MutableRequest()->MutableYdbParameters()->insert(YdbParameters->begin(), YdbParameters->end());
        }

        if (QueryCachePolicy) {
            Record.MutableRequest()->MutableQueryCachePolicy()->CopyFrom(*QueryCachePolicy);
        }

        if (CollectStats) {
            Record.MutableRequest()->SetCollectStats(CollectStats);
        }

        if (!YqlText.empty()) {
            Record.MutableRequest()->SetQuery(YqlText);
        }

        if (!QueryId.empty()) {
            Record.MutableRequest()->SetPreparedQuery(QueryId);
        }

        Record.MutableRequest()->SetSessionId(SessionId);
        Record.MutableRequest()->SetAction(QueryAction);
        Record.MutableRequest()->SetType(QueryType);
        if (OperationParams) {
            const auto& operationTimeout = GetDuration(OperationParams->operation_timeout());
            const auto& cancelAfter = GetDuration(OperationParams->cancel_after());

            Record.MutableRequest()->SetCancelAfterMs(cancelAfter.MilliSeconds());
            Record.MutableRequest()->SetTimeoutMs(operationTimeout.MilliSeconds());
        }

        RequestCtx.reset();
    }
}

}

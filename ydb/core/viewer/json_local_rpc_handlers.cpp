#include "json_handlers.h"
#include "operation_cancel.h"
#include "operation_forget.h"
#include "operation_get.h"
#include "operation_list.h"
#include "query_execute_script.h"
#include "query_fetch_script.h"
#include "scheme_directory.h"
#include "viewer_commit_offset.h"
#include "viewer_describe_consumer.h"
#include "viewer_describe_replication.h"
#include "viewer_describe_topic.h"
#include "viewer_describe_transfer.h"

namespace NKikimr::NViewer {

void InitOperationLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/operation/get", new TJsonHandler<TOperationGet>(TOperationGet::GetSwagger()));
    jsonHandlers.AddHandler("/operation/list", new TJsonHandler<TOperationList>(TOperationList::GetSwagger()), 2);
    jsonHandlers.AddHandler("/operation/cancel", new TJsonHandler<TOperationCancel>(TOperationCancel::GetSwagger()));
    jsonHandlers.AddHandler("/operation/forget", new TJsonHandler<TOperationForget>(TOperationForget::GetSwagger()));
}

void InitQueryLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/query/script/execute", new TJsonHandler<TQueryExecuteScript>(TQueryExecuteScript::GetSwagger()));
    jsonHandlers.AddHandler("/query/script/fetch", new TJsonHandler<TQueryFetchScript>(TQueryFetchScript::GetSwagger()));
}

void InitSchemeLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/scheme/directory", new TJsonSchemeDirectoryHandler(), 2);
}

void InitViewerLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/viewer/describe_replication", new TJsonHandler<TJsonDescribeReplication>(TJsonDescribeReplication::GetSwagger()));
    jsonHandlers.AddHandler("/viewer/describe_topic", new TJsonHandler<TJsonDescribeTopic>(TJsonDescribeTopic::GetSwagger()));
    jsonHandlers.AddHandler("/viewer/describe_transfer", new TJsonHandler<TJsonDescribeTransfer>(TJsonDescribeTransfer::GetSwagger()));
    jsonHandlers.AddHandler("/viewer/describe_consumer", new TJsonHandler<TJsonDescribeConsumer>(TJsonDescribeConsumer::GetSwagger()));
    jsonHandlers.AddHandler("/viewer/commit_offset", new TJsonHandler<TJsonCommitOffset>(TJsonCommitOffset::GetSwagger()));
}

}

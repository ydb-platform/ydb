#pragma once
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>


namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const TString& executionId);
TMaybe<TString> ScriptExecutionIdFromOperation(const TString& operationId, TString& error);
TMaybe<TString> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId, TString& error);

} // namespace NKikimr::NKqp

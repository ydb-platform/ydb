#pragma once
#include <ydb/public/lib/operation_id/operation_id.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>


namespace NKikimr::NKqp {

TString ScriptExecutionOperationFromExecutionId(const TString& executionId);
TMaybe<TString> ScriptExecutionIdFromOperation(const TString& operationId);
TMaybe<TString> ScriptExecutionIdFromOperation(const NOperationId::TOperationId& operationId);

} // namespace NKikimr::NKqp

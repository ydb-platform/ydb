#pragma once


namespace NYql {

void SetCurrentOperationId(const char* operationId);

long GetRunnigThreadsCount();

} // namespace NYql

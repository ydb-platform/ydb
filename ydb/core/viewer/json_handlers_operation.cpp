#include "json_handlers.h"
#include "operation_cancel.h"
#include "operation_forget.h"
#include "operation_get.h"
#include "operation_list.h"

namespace NKikimr::NViewer {

void InitOperationGetJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/operation/get", new TJsonHandler<TOperationGet>(TOperationGet::GetSwagger()));
}

void InitOperationListJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/operation/list", new TJsonHandler<TOperationList>(TOperationList::GetSwagger()));
}

void InitOperationCancelJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/operation/cancel", new TJsonHandler<TOperationCancel>(TOperationCancel::GetSwagger()));
}

void InitOperationForgetJsonHandler(TJsonHandlers& handlers) {
    handlers.AddHandler("/operation/forget", new TJsonHandler<TOperationForget>(TOperationForget::GetSwagger()));
}

void InitOperationJsonHandlers(TJsonHandlers& jsonHandlers) {
    InitOperationGetJsonHandler(jsonHandlers);
    InitOperationListJsonHandler(jsonHandlers);
    InitOperationCancelJsonHandler(jsonHandlers);
    InitOperationForgetJsonHandler(jsonHandlers);
}

} // namespace NKikimr::NViewer

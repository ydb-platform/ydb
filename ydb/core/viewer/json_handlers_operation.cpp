#include "json_handlers.h"

#include "operation_get.h"
#include "operation_list.h"
#include "operation_cancel.h"
#include "operation_forget.h"

namespace NKikimr::NViewer {

void InitOperationJsonHandlers(TJsonHandlers& jsonHandlers) {
    jsonHandlers.AddHandler("/operation/get", new TJsonHandler<TOperationGet>);
    jsonHandlers.AddHandler("/operation/list", new TJsonHandler<TOperationList>);
    jsonHandlers.AddHandler("/operation/cancel", new TJsonHandler<TOperationCancel>);
    jsonHandlers.AddHandler("/operation/forget", new TJsonHandler<TOperationForget>);
}

}

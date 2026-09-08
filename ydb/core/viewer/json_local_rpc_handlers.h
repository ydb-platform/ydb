#pragma once

namespace NKikimr::NViewer {

struct TJsonHandlers;

void InitOperationLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers);
void InitQueryLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers);
void InitSchemeLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers);
void InitViewerLocalRpcJsonHandlers(TJsonHandlers& jsonHandlers);

}

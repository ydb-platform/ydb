#include "schemeshard__data_erasure_manager.h"

namespace NKikimr::NSchemeShard {

TDataErasureManager::TDataErasureManager(TSchemeShard* const schemeShard)
    : SchemeShard(schemeShard)
{}

TDataErasureManager::EStatus TDataErasureManager::GetStatus() const {
    return Status;
}

void TDataErasureManager::SetStatus(const EStatus& status) {
    Status = status;
}

void TDataErasureManager::IncGeneration() {
    ++Generation;
}

void TDataErasureManager::SetGeneration(ui64 generation) {
    Generation = generation;
}

ui64 TDataErasureManager::GetGeneration() const {
    return Generation;
}

void TDataErasureManager::Clear() {
    ClearOperationQueue();
    ClearWaitingDataErasureRequests();
}

} // NKikimr::NSchemeShard

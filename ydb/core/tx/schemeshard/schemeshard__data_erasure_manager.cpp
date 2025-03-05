#include "schemeshard__data_erasure_manager.h"

namespace NKikimr::NSchemeShard {

TDataErasureManager::TDataErasureManager(TSchemeShard* const schemeShard)
    : SchemeShard(schemeShard)
{}

EDataErasureStatus TDataErasureManager::GetStatus() const {
    return Status;
}

void TDataErasureManager::SetStatus(const EDataErasureStatus& status) {
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

void TDataErasureManager::Start() {
    Running = true;
}

void TDataErasureManager::Stop() {
    Running = false;
}

bool TDataErasureManager::IsRunning() const {
    return Running;
}

} // NKikimr::NSchemeShard

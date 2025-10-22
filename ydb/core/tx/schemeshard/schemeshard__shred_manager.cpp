#include "schemeshard__shred_manager.h"

namespace NKikimr::NSchemeShard {

TShredManager::TShredManager(TSchemeShard* const schemeShard)
    : SchemeShard(schemeShard)
{}

EShredStatus TShredManager::GetStatus() const {
    return Status;
}

void TShredManager::SetStatus(const EShredStatus& status) {
    Status = status;
}

void TShredManager::IncGeneration() {
    ++Generation;
}

void TShredManager::SetGeneration(ui64 generation) {
    Generation = generation;
}

ui64 TShredManager::GetGeneration() const {
    return Generation;
}

void TShredManager::Clear() {
    ClearOperationQueue();
    ClearWaitingShredRequests();
}

void TShredManager::Start() {
    Running = true;
}

void TShredManager::Stop() {
    Running = false;
}

bool TShredManager::IsRunning() const {
    return Running;
}

} // NKikimr::NSchemeShard

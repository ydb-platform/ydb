#include "rt_insertion.h"

namespace NKikimr::NOlap {

void TInsertionSummary::RemovePriority(const TPathInfo& pathInfo) noexcept {
    const ui64 priority = pathInfo.GetIndexationPriority();
    auto it = Priorities.find(priority);
    if (it == Priorities.end()) {
        Y_VERIFY(priority == 0);
        return;
    }
    Y_VERIFY(it->second.erase(&pathInfo) || priority == 0);
    if (it->second.empty()) {
        Priorities.erase(it);
    }
}

void TInsertionSummary::AddPriority(const TPathInfo& pathInfo) noexcept {
    Y_VERIFY(Priorities[pathInfo.GetIndexationPriority()].emplace(&pathInfo).second);
}

NKikimr::NOlap::TPathInfo& TInsertionSummary::GetPathInfo(const ui64 pathId) {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        it = PathInfo.emplace(pathId, TPathInfo(*this, pathId)).first;
    }
    return it->second;
}

std::optional<NKikimr::NOlap::TPathInfo> TInsertionSummary::ExtractPathInfo(const ui64 pathId) {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        return {};
    }
    RemovePriority(it->second);
    std::optional<TPathInfo> result = std::move(it->second);
    PathInfo.erase(it);
    return result;
}

NKikimr::NOlap::TPathInfo* TInsertionSummary::GetPathInfoOptional(const ui64 pathId) {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        return nullptr;
    }
    return &it->second;
}

const NKikimr::NOlap::TPathInfo* TInsertionSummary::GetPathInfoOptional(const ui64 pathId) const {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        return nullptr;
    }
    return &it->second;
}

bool TInsertionSummary::IsOverloaded(const ui64 pathId) const {
    auto it = PathInfo.find(pathId);
    if (it == PathInfo.end()) {
        return false;
    }
    return it->second.IsOverloaded();
}

void TInsertionSummary::Clear() {
    PathInfo.clear();
    Priorities.clear();
}

}

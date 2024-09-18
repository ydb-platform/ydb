#include <mutex>
#include <utility>
#include <util/generic/string.h>
#include "context.h"
#include "context_storage.h"

namespace NMVP {
namespace NOIDC {

TContextStorage::TContextStorage()
    : RefreshQueue([] (const std::shared_ptr<std::pair<TString, TContextRecord>>& left, const std::shared_ptr<std::pair<TString, TContextRecord>>& right) {
        return left->second.GetExpirationTime() > right->second.GetExpirationTime();
    })
{}

void TContextStorage::Write(const TContext& context) {
    std::lock_guard<std::mutex> guard(Mutex);
    auto res = Contexts.insert(std::make_pair(context.GetState(), TContextRecord(context)));
    RefreshQueue.push(std::make_shared<std::pair<TString, TContextRecord>>(*res.first));
}

std::pair<bool, TContextRecord> TContextStorage::Find(const TString& state) {
    std::lock_guard<std::mutex> guard(Mutex);
    auto it = Contexts.find(state);
    if (it == Contexts.end()) {
        return std::make_pair(false, TContextRecord(TContext()));
    }
    std::pair<bool, TContextRecord> result = std::make_pair(true, it->second);
    return result;
}

void TContextStorage::Refresh(TInstant now) {
    std::lock_guard<std::mutex> guard(Mutex);
    while (!RefreshQueue.empty() && RefreshQueue.top()->second.GetExpirationTime() + Ttl <= now) {
        TString key = RefreshQueue.top()->first;
        RefreshQueue.pop();
        Contexts.erase(key);
    }
}

void TContextStorage::SetTtl(const TDuration& ttl) {
    Ttl = ttl;
}

} // NOIDC
} // NMVP

#include <mutex>
#include <utility>
#include <util/generic/string.h>
#include "context.h"
#include "context_storage.h"

namespace NMVP {
namespace NOIDC {

void TContextStorage::Write(const TContext& context) {
    std::lock_guard<std::mutex> guard(Mutex);
    Contexts.insert(std::make_pair(context.GetState(), TContextRecord(context)));
}

std::pair<bool, TContextRecord> TContextStorage::Find(const TString& state) {
    std::lock_guard<std::mutex> guard(Mutex);
    auto it = Contexts.find(state);
    if (it == Contexts.end()) {
        return std::make_pair(false, TContextRecord(TContext()));
    }
    std::pair<bool, TContextRecord> result = std::make_pair(true, it->second);
    Contexts.erase(it);
    return result;
}

} // NOIDC
} // NMVP

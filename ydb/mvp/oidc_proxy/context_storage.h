#pragma once

#include <util/generic/string.h>
#include <util/datetime/base.h>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <memory>
#include <functional>
#include <queue>
#include "context.h"

namespace NMVP {
namespace NOIDC {

class TContextStorage {
private:
    using Compare = std::function<bool(const std::shared_ptr<std::pair<TString, TContextRecord>>&, const std::shared_ptr<std::pair<TString, TContextRecord>>&)>;

    std::unordered_map<TString, TContextRecord> Contexts;
    std::priority_queue<std::shared_ptr<std::pair<TString, TContextRecord>>, std::vector<std::shared_ptr<std::pair<TString, TContextRecord>>>, Compare> RefreshQueue;
    TDuration Ttl = TDuration::Minutes(10);
    std::mutex Mutex;

public:
    TContextStorage();

    void Write(const TContext& context);
    std::pair<bool, TContextRecord> Find(const TString& state);
    void Refresh(TInstant now);
    void SetTtl(const TDuration& ttl);
};

} // NOIDC
} // NMVP

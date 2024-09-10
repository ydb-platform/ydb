#pragma once

#include <util/generic/string.h>
#include <mutex>
#include <unordered_map>
#include <utility>
#include "context.h"

namespace NMVP {
namespace NOIDC {

class TContextStorage {
private:
    std::unordered_map<TString, TContextRecord> Contexts;
    std::mutex Mutex;

public:
    void Write(const TContext& context);
    std::pair<bool, TContextRecord> Find(const TString& state);
};

} // NOIDC
} // NMVP

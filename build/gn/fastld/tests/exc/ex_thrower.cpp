#include "exc.h"
#include <stdexcept>
void throw_derived() { throw Derived(); }
void throw_vague(int v) { throw Vague{v}; }
void throw_runtime(const char *msg) { throw std::runtime_error(msg); }
VBase *make_vder() { return new VDer; }
const std::type_info *typeid_vague() { return &typeid(Vague); }
const std::type_info *typeid_vder() { return &typeid(VDer); }
void call_cb(void (*cb)()) { cb(); }
// Throw and catch during static initialization (before main, while the loader is initializing).
static int caught_at_init = [] {
    try {
        throw_vague(5);
    } catch (const Vague &v) {
        return v.v;
    }
    return -1;
}();
int static_init_caught() { return caught_at_init; }

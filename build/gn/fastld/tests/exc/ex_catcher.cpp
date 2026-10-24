#include "exc.h"
#include <cstdio>
#include <stdexcept>
static char buf[256];
const char *catcher_report() {
    int n = 0;
    try {
        throw_vague(11);
    } catch (const Vague &v) {
        n += std::snprintf(buf + n, sizeof buf - n, "Vague=%d ", v.v);
    }
    try {
        throw_derived();
    } catch (const Base &b) {
        n += std::snprintf(buf + n, sizeof buf - n, "Base&=%s ", b.name());
    }
    try {
        throw_runtime("rt");
    } catch (const std::exception &e) {
        n += std::snprintf(buf + n, sizeof buf - n, "std::exception=%s ", e.what());
    }
    VBase *p = make_vder();
    n += std::snprintf(buf + n, sizeof buf - n, "dynamic_cast=%s", dynamic_cast<VDer *>(p) ? "ok" : "NULL");
    delete p;
    return buf;
}
std::exception_ptr capture_runtime() {
    try {
        throw_runtime("captured");
    } catch (...) {
        return std::current_exception();
    }
    return nullptr;
}

#include "exc.h"
#include <cstdio>
#include <stdexcept>
#include <string>
#include <thread>
int main() {
    std::printf("static-init throw/catch in ex_thrower: %d\n", static_init_caught());
    try {
        throw_derived();
    } catch (const Base &b) {
        std::printf("exe caught Base& from ex_thrower: %s\n", b.name());
    }
    try {
        throw_vague(3);
    } catch (const Vague &v) {
        std::printf("exe caught Vague from ex_thrower: %d\n", v.v);
    }
    std::printf("ex_catcher: %s\n", catcher_report());
    try {
        throw_runtime("boom");
    } catch (const std::runtime_error &e) {
        std::printf("exe caught runtime_error: %s\n", e.what());
    }
    try {
        std::string s;
        (void)s.at(5);
    } catch (const std::out_of_range &) {
        std::printf("exe caught out_of_range thrown inside libcxx.so\n");
    }
    try {
        call_cb([] { throw Vague{7}; });
    } catch (const Vague &v) {
        std::printf("exe caught Vague through ex_thrower frames: %d\n", v.v);
    }
    try {
        std::rethrow_exception(capture_runtime());
    } catch (const std::exception &e) {
        std::printf("exe rethrew exception_ptr from ex_catcher: %s\n", e.what());
    }
    VBase *p = make_vder();
    std::printf("exe dynamic_cast<VDer*>: %s, typeid(*p)==typeid(VDer): %s\n", dynamic_cast<VDer *>(p) ? "ok" : "NULL",
                typeid(*p) == typeid(VDer) ? "yes" : "no");
    std::printf("&typeid(Vague) unique: %s, &typeid(VDer) unique: %s\n", typeid_vague() == &typeid(Vague) ? "yes" : "no",
                typeid_vder() == &typeid(VDer) ? "yes" : "no");
    delete p;
    std::thread t([] {
        try {
            throw_vague(9);
        } catch (const Vague &v) {
            std::printf("thread caught Vague: %d\n", v.v);
        }
    });
    t.join();
    try {
        throw 42;
    } catch (int i) {
        std::printf("exe caught int: %d\n", i);
    }
    return 0;
}

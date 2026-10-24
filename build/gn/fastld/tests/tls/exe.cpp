#include "tls.h"
#include <atomic>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <pthread.h>
#include <thread>
#include <vector>

thread_local int e_tls = 303;
thread_local char e_buf[32] = "exe-init";
thread_local int e_bss;
struct EObj {
    int v = 66;
    ~EObj() { std::printf("  ~EObj v=%d\n", v); }
};
thread_local EObj e_obj;

static void report(const char *who) {
    std::printf("[%s] exe: e_tls=%d e_buf=%s e_bss=%d e_obj=%d\n", who, e_tls, e_buf, e_bss, e_obj.v);
    std::printf("[%s] exe->a: a_tls=%d a_str=%s a_bss=%d\n", who, a_tls, a_str, a_bss);
    std::printf("[%s] b->a: %d, b_tls=%ld, a_local_counter=%d, a_obj=%d\n", who, b_read_a_tls(),
                b_tls_value(), a_local_counter(), a_obj_value());
    std::printf("[%s] ie_tls=%d\n", who, ie_get());
}
static void mutate(int k) {
    e_tls += k;
    std::strcpy(e_buf, "exe-set");
    e_bss = k;
    e_obj.v += k;
    a_set(1000 + k);
    b_write_a_tls(a_tls + 1);
    b_inc();
    ie_inc();
}
static void thread_body(const char *who, int k) {
    report(who);
    mutate(k);
    report(who);
}
static void *pthread_body(void *) {
    thread_body("pthread", 20);
    return nullptr;
}

typedef int (*p_get_t)();
typedef void (*p_inc_t)();
static p_get_t p_get;
static p_inc_t p_inc;

int main() {
    thread_body("main", 10);
    std::thread t1(thread_body, "std::thread", 30);
    t1.join();
    pthread_t pt;
    pthread_create(&pt, nullptr, pthread_body, nullptr);
    pthread_join(pt, nullptr);

    // A thread that exists before the plugin with TLS is loaded.
    std::atomic<int> stage{0};
    std::thread early([&] {
        while (stage.load() == 0) std::this_thread::yield();
        std::printf("[early] p_get=%d\n", p_get());
        p_inc();
        std::printf("[early] p_get after inc=%d\n", p_get());
    });
    void *h = dlopen("libtls_plugin.so", RTLD_NOW);
    if (!h) { std::printf("dlopen failed\n"); return 1; }
    p_get = (p_get_t)dlsym(h, "p_get");
    p_inc = (p_inc_t)dlsym(h, "p_inc");
    std::printf("[main] p_get=%d\n", p_get());
    p_inc();
    stage = 1;
    early.join();
    std::printf("[main] p_get=%d\n", p_get());
    std::thread late([] { std::printf("[late] p_get=%d\n", p_get()); });
    late.join();

    // Concurrent threads: each must see its own copies.
    std::atomic<int> bad{0};
    std::vector<std::thread> ts;
    for (int i = 0; i < 8; ++i)
        ts.emplace_back([&bad, i] {
            for (int j = 0; j < 10000; ++j) { ++e_tls; ++a_tls; b_inc(); p_inc(); ie_inc(); }
            if (e_tls != 303 + 10000 || a_tls != 101 + 10000 || b_tls_value() != 202 + 10000 ||
                p_get() != 404 + 1 + 10000 || ie_get() != 909 + 10000)
                ++bad;
            (void)i;
        });
    for (auto &t : ts) t.join();
    std::printf("8 concurrent threads, bad=%d\n", bad.load());
    std::printf("main end\n");
    return 0;
}

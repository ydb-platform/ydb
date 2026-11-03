// The same scheduler and the same command language as kqp_scheduler_sim, but linked into the caller's process:
// no pipe, no process switch and no per-command round-trip. Used by sim5.py through ctypes.
//
//   void* kqp_sim_new(unsigned long long seed);
//   const char* kqp_sim_exec(void* handle, const char* line);  // the reply line, valid until the next call
//   void kqp_sim_free(void* handle);
//
// The commands are those of main.cpp (see the comment there); an error is replied as "error <message>".

#include "stand.h"

namespace {

thread_local TString Reply;

}  // namespace

extern "C" {

__attribute__((visibility("default"))) void* kqp_sim_new(unsigned long long seed) {
    try {
        return new NKikimr::NKqp::NStand::TStand(seed);
    } catch (const std::exception&) {
        return nullptr;
    }
}

__attribute__((visibility("default"))) void kqp_sim_free(void* handle) {
    if (!handle) {
        return;
    }
    auto& stand = *static_cast<NKikimr::NKqp::NStand::TStand*>(handle);
    // Like the "quit" of the line protocol: a task must not be destroyed while it holds the quota
    TStringBuilder reply;
    try {
        stand.Handle("quit", reply);
    } catch (const std::exception&) {
    }
    delete &stand;
}

__attribute__((visibility("default"))) const char* kqp_sim_exec(void* handle, const char* line) {
    auto& stand = *static_cast<NKikimr::NKqp::NStand::TStand*>(handle);

    TStringBuilder reply;
    bool next = true;
    try {
        next = stand.Handle(line, reply);
    } catch (const std::exception& e) {
        reply.clear();
        reply << "error " << TString(e.what()).substr(0, 500);
    }

    if (!next) {
        reply.clear();
        reply << "ok";
    } else {
        stand.AppendWoken(reply);
    }

    // The newlines would break the one-line protocol
    for (auto& c : reply) {
        if (c == '\n') {
            c = ' ';
        }
    }
    Reply = TString(reply);
    return Reply.data();
}

}  // extern "C"

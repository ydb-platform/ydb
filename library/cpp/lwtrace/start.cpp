#include "start.h"

#include "all.h"

#include <google/protobuf/text_format.h>

#include <util/generic/singleton.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/system/env.h>

#include <stdlib.h>

using namespace NLWTrace;

namespace {
    struct TTraceManagerHolder {
        TManager TraceManager;
        TTraceManagerHolder()
            : TraceManager(*Singleton<TProbeRegistry>(), true)
        {
        }
    };

    void TraceFromEnv(TString path) {
        TString script = TUnbufferedFileInput(path).ReadAll();
        TQuery query;
        bool ok = google::protobuf::TextFormat::ParseFromString(script, &query);
        Y_ABORT_UNLESS(ok, "failed to parse protobuf");
        Singleton<TTraceManagerHolder>()->TraceManager.New("env", query);
    }

} // anonymous namespace

void NLWTrace::StartLwtraceFromEnv() {
    static bool started = false;
    if (started) {
        return;
    } else {
        started = true;
    }

    TString path = GetEnv("LWTRACE");
    if (!path) {
        return;
    }

    try {
        TraceFromEnv(path);
    } catch (...) {
        Cerr << "failed to load lwtrace script: " << CurrentExceptionMessage() << "\n";
        abort();
    }
}

void NLWTrace::StartLwtraceFromEnv(std::function<void(TManager&)> prepare) {
    TString path = GetEnv("LWTRACE");
    if (Y_LIKELY(!path)) {
        return;
    }

    try {
        prepare(Singleton<TTraceManagerHolder>()->TraceManager);
        TraceFromEnv(path);
    } catch (...) {
        Cerr << "failed to load lwtrace script: " << CurrentExceptionMessage() << "\n";
        abort();
    }
}

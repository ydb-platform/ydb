#pragma once

#include "trace_json.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>

#include <google/protobuf/util/json_util.h>
#include <library/cpp/threading/future/future.h>
#include <util/stream/file.h>

namespace NKqpRun {

// Streams OTLP JSON without retaining the run in memory. Each complete line is
// independently readable, including when a benchmark process is interrupted.
class TTraceFileUploader: public NActors::TActorBootstrapped<TTraceFileUploader> {
public:
    struct TEvFlush: NActors::TEventLocal<TEvFlush, EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 100> {
        NThreading::TPromise<void> Promise = NThreading::NewPromise<void>();
    };

    explicit TTraceFileUploader(const TString& path, const TString& dataset)
        : Output_(path)
    {
        opentelemetry::proto::common::v1::AnyValue value;
        value.set_string_value(dataset);
        TString json;
        Y_ENSURE(google::protobuf::util::MessageToJsonString(value, &json).ok());
        Prefix_ = "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"dataset.name\",\"value\":" + json +
                  "}]},\"scopeSpans\":[{\"scope\":{\"name\":\"kqprun\"},\"spans\":[";
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    void Handle(NWilson::TEvWilson::TPtr& ev) {
        if (Error_) {
            return;
        }
        try {
            const TString json = SpanToOtlpJson(ev->Get()->Span);
            Output_ << Prefix_ << json << "]}]}]}\n";
            Output_.Flush();
        } catch (...) {
            Error_ = CurrentExceptionMessage();
        }
    }

    void Handle(TEvFlush::TPtr& ev) {
        if (Error_) {
            ev->Get()->Promise.SetException(Error_);
        } else {
            ev->Get()->Promise.SetValue();
        }
    }

    STRICT_STFUNC(StateWork,
        hFunc(NWilson::TEvWilson, Handle);
        hFunc(TEvFlush, Handle);
    );

private:
    TFileOutput Output_;
    TString Prefix_;
    TString Error_;
};

}   // namespace NKqpRun

#pragma once

#include <ydb/library/actors/wilson/wilson_uploader.h>

class TFakeWilsonUploader
    : public NActors::TActorBootstrapped<TFakeWilsonUploader>
{
public:
    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    void Handle(NWilson::TEvWilson::TPtr ev) {
        Spans.push_back(std::move(ev->Get()->Span));
    }

    STRICT_STFUNC(StateWork,
        hFunc(NWilson::TEvWilson, Handle);
    );

public:
    using TOtelSpan = opentelemetry::proto::trace::v1::Span;
    std::vector<TOtelSpan> Spans;
};

inline NActors::IActor* CreateFakeWilsonUploader() {
    return new TFakeWilsonUploader;
}

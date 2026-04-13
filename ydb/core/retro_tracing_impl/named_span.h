#pragma once

#include "retro_tracing.h"

#include <ydb/library/actors/retro_tracing/typed_retro_span.h>

namespace NKikimr {

class TNamedSpan final : public NRetroTracing::TTypedRetroSpan<TNamedSpan, NamedSpan> {
public:
    void SetName(const char* name);
    TString GetName() const override;
    std::unique_ptr<NWilson::TSpan> MakeWilsonSpan() override;

public:
    constexpr static size_t MaxNameSize = 64 - sizeof(ui32);

public:
    size_t NameSize;
    char NameBuffer[MaxNameSize];
};

} // namespace NKikimr

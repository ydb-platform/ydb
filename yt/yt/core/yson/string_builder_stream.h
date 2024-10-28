#pragma once

#include <library/cpp/yt/string/string_builder.h>

#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TStringBuilderStream final
    : public IOutputStream
{
public:
    explicit TStringBuilderStream(TStringBuilderBase* builder) noexcept;

private:
    TStringBuilderBase* const Builder_;

    void DoWrite(const void* data, size_t size) final;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

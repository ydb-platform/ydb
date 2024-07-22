#pragma once

#include <library/cpp/yt/string/string_builder.h>

#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TStringBuilderStream final
    : public IOutputStream
{
public:
    TStringBuilderStream(TStringBuilderBase* builder) noexcept;

private:
    TStringBuilderBase* Builder_;

    void DoWrite(const void* data, size_t size) final;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

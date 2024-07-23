#include "string_builder_stream.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TStringBuilderStream::TStringBuilderStream(TStringBuilderBase* builder) noexcept
    : Builder_(builder)
{ }

void TStringBuilderStream::DoWrite(const void* data, size_t size)
{
    Builder_->AppendString(TStringBuf(static_cast<const char*>(data), size));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

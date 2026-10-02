#include "helpers.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

void WriteMessage(TTaggedPayloadWriter* writer, TStringBuf message)
{
    writer->BeginMessage()->AppendString(message);
    writer->EndMessage();
}

void WriteTag(TTaggedPayloadWriter* writer, TStringBuf key, TStringBuf value)
{
    writer->AppendTag(key, [&] (TStringBuilderBase* builder) {
        builder->AppendString(value);
    });
}

void WriteWellKnownTag(TTaggedPayloadWriter* writer, TStringBuf key, TStringBuf value)
{
    writer->AppendWellKnownTag(key, [&] (TStringBuilderBase* builder) {
        builder->AppendString(value);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

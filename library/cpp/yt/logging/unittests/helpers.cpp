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
    writer->BeginTag(key)->AppendString(value);
    writer->EndTag();
}

void WriteWellKnownTag(TTaggedPayloadWriter* writer, TStringBuf key, TStringBuf value)
{
    writer->BeginWellKnownTag(key)->AppendString(value);
    writer->EndTag();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

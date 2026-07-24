#pragma once

#include <library/cpp/yt/logging/tagged_payload.h>

#include <util/generic/strbuf.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! Writes the message (equivalent to BeginMessage, an append, and EndMessage).
void WriteMessage(TTaggedPayloadWriter* writer, TStringBuf message);

//! Writes a tag with a ready string value (equivalent to BeginTag, an append, and EndTag).
void WriteTag(TTaggedPayloadWriter* writer, TStringBuf key, TStringBuf value);

//! Writes a well-known tag with a ready string value (see BeginWellKnownTag).
void WriteWellKnownTag(TTaggedPayloadWriter* writer, TStringBuf key, TStringBuf value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

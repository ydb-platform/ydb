#include "tag.h"

#include "tagged_payload.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, TLoggingTagListPayloadView tags, TStringBuf /*spec*/)
{
    TTaggedPayloadReader reader(tags);
    bool first = true;
    while (auto tag = reader.TryReadTag()) {
        if (!first) {
            builder->AppendString(", "_sb);
        }
        first = false;
        builder->AppendString(tag->Key);
        builder->AppendString(": "_sb);
        builder->AppendString(tag->Value);
    }
}

void FormatValue(TStringBuilderBase* builder, const TLoggingTagListPayload& tags, TStringBuf spec)
{
    FormatValue(builder, AsView(tags), spec);
}

void FormatValue(TStringBuilderBase* builder, const TLoggingTagList& tags, TStringBuf spec)
{
    FormatValue(builder, tags.GetPayload(), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

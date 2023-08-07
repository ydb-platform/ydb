#include "convert.h"

#include <yt/yt/core/yson/tokenizer.h>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

template <>
TYsonString ConvertToYsonStringNestingLimited(const TYsonString& value, int nestingLevelLimit)
{
    TMemoryInput input(value.AsStringBuf());
    TYsonPullParser parser(&input, value.GetType(), nestingLevelLimit);
    TYsonPullParserCursor cursor(&parser);

    TStringStream stream;
    stream.Str().reserve(value.AsStringBuf().size());
    {
        TCheckedInDebugYsonTokenWriter writer(&stream, value.GetType(), nestingLevelLimit);
        cursor.TransferComplexValue(&writer);
        writer.Finish();
    }
    return TYsonString(std::move(stream.Str()), value.GetType());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYSon

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

const TToken& SkipAttributes(TTokenizer* tokenizer)
{
    int depth = 0;
    while (true) {
        tokenizer->ParseNext();
        const auto& token = tokenizer->CurrentToken();
        switch (token.GetType()) {
            case ETokenType::LeftBrace:
            case ETokenType::LeftAngle:
                ++depth;
                break;

            case ETokenType::RightBrace:
            case ETokenType::RightAngle:
                --depth;
                break;

            default:
                if (depth == 0) {
                    return token;
                }
                break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree


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

} // namespace NYT::NYson

namespace NYT::NYTree::NDetail {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

double ConvertYsonStringBufToDouble(const NYson::TYsonStringBuf& yson)
{
    using namespace NYT::NYTree;

    NYson::TTokenizer tokenizer(yson.AsStringBuf());
    tokenizer.SkipAttributes();
    const auto& token = tokenizer.CurrentToken();
    switch (token.GetType()) {
        case NYson::ETokenType::Int64:
            return token.GetInt64Value();
        case NYson::ETokenType::Double:
            return token.GetDoubleValue();
        case NYson::ETokenType::Boolean:
            return token.GetBooleanValue();
        default:
            THROW_ERROR_EXCEPTION("Cannot parse \"double\" from %Qlv",
                token.GetType())
                << TErrorAttribute("data", yson.AsStringBuf());
    }
}

TString ConvertYsonStringBufToString(const NYson::TYsonStringBuf& yson)
{
    using namespace NYT::NYTree;

    NYson::TTokenizer tokenizer(yson.AsStringBuf());
    tokenizer.SkipAttributes();
    const auto& token = tokenizer.CurrentToken();
    switch (token.GetType()) {
        case NYson::ETokenType::String: {
            TString result(token.GetStringValue());
            tokenizer.ParseNext();
            tokenizer.CurrentToken().ExpectType(NYson::ETokenType::EndOfStream);
            return result;
        }
        default:
            THROW_ERROR_EXCEPTION("Cannot parse \"string\" from %Qlv",
                token.GetType())
                << TErrorAttribute("data", yson.AsStringBuf());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NDetail


#include "yql_yt_yson_helpers.h"

namespace NYql::NFmr {

TString GetBinaryYson(const TString& textYsonContent) {
    TStringStream binaryYsonInputStream;
    TStringStream textYsonInputStream(textYsonContent);
    NYson::ReformatYsonStream(&textYsonInputStream, &binaryYsonInputStream, NYson::EYsonFormat::Binary, ::NYson::EYsonType::ListFragment);
    return binaryYsonInputStream.ReadAll();
}

TString GetTextYson(const TString& binaryYsonContent) {
    TStringStream binaryYsonInputStream(binaryYsonContent);
    TStringStream textYsonInputStream;
    NYson::ReformatYsonStream(&binaryYsonInputStream, &textYsonInputStream, NYson::EYsonFormat::Text, ::NYson::EYsonType::ListFragment);
    return textYsonInputStream.ReadAll();
}

} // namespace NYql::NFmr

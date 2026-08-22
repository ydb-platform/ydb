#include "yql_yt_yson_helpers.h"

namespace NYql::NFmr {

TString GetBinaryYson(const TString& textYsonContent, NYson::EYsonType nodeType) {
    TString result;
    TStringInput textYsonInput(textYsonContent);
    TStringOutput binaryYsonOutput(result);
    NYson::ReformatYsonStream(&textYsonInput, &binaryYsonOutput, NYson::EYsonFormat::Binary, nodeType);
    return result;
}

TString GetTextYson(const TString& binaryYsonContent) {
    TString result;
    TStringInput binaryYsonInput(binaryYsonContent);
    TStringOutput textYsonOutput(result);
    NYson::ReformatYsonStream(&binaryYsonInput, &textYsonOutput, NYson::EYsonFormat::Text, ::NYson::EYsonType::ListFragment);
    return result;
}

} // namespace NYql::NFmr

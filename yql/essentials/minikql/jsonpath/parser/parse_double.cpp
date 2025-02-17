
#include "parse_double.h"

#include <contrib/libs/double-conversion/double-conversion/double-conversion.h>

#include <cmath>

namespace NYql::NJsonPath {

using double_conversion::StringToDoubleConverter;

double ParseDouble(const TStringBuf literal) {
    // FromString<double> from util/string/cast.h is permissive to junk in string.
    // In our case junk in string means bug in grammar.
    // See https://a.yandex-team.ru/arc/trunk/arcadia/util/string/cast.cpp?rev=6456750#L692
    struct TStringToNumberConverter: public StringToDoubleConverter {
        inline TStringToNumberConverter()
            : StringToDoubleConverter(
                NO_FLAGS,
                /* empty_string_value */ 0.0,
                /* junk_string_value */ NAN,
                /* infinity_symbol */ nullptr,
                /* nan_symbol */ nullptr
            )
        {
        }
    };

    int parsedCharactersCount = 0;
    return Singleton<TStringToNumberConverter>()->StringToDouble(literal.data(), literal.length(), &parsedCharactersCount);
}

}

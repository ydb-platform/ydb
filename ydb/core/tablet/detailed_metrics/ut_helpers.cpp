#include "ut_helpers.h"

#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace NDetailedMetricsTests {

TString NormalizeJson(const TString& jsonString) {
    NJson::TJsonValue parsedJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(jsonString), &parsedJson));

    // NOTE: The prettifier is needed here to make sure all brackets (both [] and {})
    //       are aligned "Python style" with the opening bracket placed on the starting line.
    //       By default, WriteJson() places opening brackets on a separate line and makes
    //       all the inner strings double-aligned, which takes too many lines
    //       and makes it harder for humans to read.
    return NJson::PrettifyJson(
        NJson::WriteJson(
            parsedJson,
            true /* formatOutput */,
            true /* sortkeys */
        ),
        false /* unquote */,
        2 /* padding */
    );
}

} // namespace NDetailedMetricsTests

} // namespace NKikimr

#include "ut_helpers.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

namespace NDetailedMetricsTests {

TString NormalizeJson(const TString& jsonString) {
    NJson::TJsonValue parsedJson;
    UNIT_ASSERT(NJson::ReadJsonTree(TStringBuf(jsonString), &parsedJson));

    return NJson::WriteJson(
        parsedJson,
        true /* formatOutput */,
        true /* sortkeys */
    );
}

} // namespace NDetailedMetricsTests

} // namespace NKikimr

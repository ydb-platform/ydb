#include "filler.h"

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

namespace NKikimr::NArrow::NConstruction {

TStringPoolFiller::TStringPoolFiller(const ui32 poolSize, const ui32 strLen, const TString& defaultValue, const double defaultValueFrq) {
    for (ui32 i = 0; i < poolSize; ++i) {
        if (RandomNumber<double>() < defaultValueFrq) {
            Data.emplace_back(defaultValue);
        } else {
            Data.emplace_back(NUnitTest::RandomString(strLen, i));
        }
    }
}

arrow::util::string_view TStringPoolFiller::GetValue(const ui32 idx) const {
    const TString& str = Data[(2 + 7 * idx) % Data.size()];
    return arrow::util::string_view(str.data(), str.size());
}

}   // namespace NKikimr::NArrow::NConstruction

#include "../pg_compat.h"

#include <library/cpp/testing/unittest/registar.h>

#include "arrow.h"
#include "arrow_impl.h"

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/types/uuid/uuid.h>

extern "C" {
#include "utils/fmgrprotos.h"
}

namespace {

using namespace NKikimr::NMiniKQL;

TGUID MakeTestGuid(ui8 fill) {
    TGUID guid;
    std::memset(&guid, fill, sizeof(guid));
    return guid;
}

TString UuidToText(const TGUID& guid) {
    TString str;
    str.reserve(36);
    ui16 dw[8];
    std::memcpy(dw, &guid, sizeof(dw));
    TStringOutput out(str);
    NKikimr::NUuid::UuidToString(dw, out);
    return str;
}

} // namespace

Y_UNIT_TEST_SUITE(TPgBlockToPgTests) {

Y_UNIT_TEST(UuidBlockConversion) {
    TScopedAlloc alloc(__LOCATION__);
    TPAllocScope scope;

    const auto guid = MakeTestGuid(0xAB);
    const TString uuidText = UuidToText(guid);

    const auto pgRes = (text*)DirectFunctionCall1Coll(
        uuid_in, DEFAULT_COLLATION_OID, PointerGetDatum(uuidText.c_str()));
    Y_DEFER { pfree(pgRes); };

    UNIT_ASSERT_VALUES_EQUAL(
        TString(DatumGetCString(DirectFunctionCall1(uuid_out, PointerGetDatum(pgRes)))),
        uuidText);
}

Y_UNIT_TEST(DyNumberBlockConversion) {
    TScopedAlloc alloc(__LOCATION__);
    TPAllocScope scope;

    const auto dy = ValueFromString(NYql::NUdf::EDataSlot::DyNumber, "-10.23");
    const auto pgNumeric = NYql::DyNumberToPgNumeric(dy);
    Y_DEFER { pfree(pgNumeric); };

    UNIT_ASSERT_VALUES_EQUAL(
        TString(DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(pgNumeric)))),
        "-10.23");
}

Y_UNIT_TEST(DyNumberBlockConversionLongValue) {
    TScopedAlloc alloc(__LOCATION__);
    TPAllocScope scope;

    constexpr TStringBuf longDyNumber = "123456789012345.67";
    const auto dy = ValueFromString(NYql::NUdf::EDataSlot::DyNumber, longDyNumber);
    UNIT_ASSERT(dy.AsStringRef().Size() > NYql::NUdf::TUnboxedValuePod::InternalBufferSize);

    const auto pgNumeric = NYql::DyNumberToPgNumeric(MakeString(dy.AsStringRef()));
    Y_DEFER { pfree(pgNumeric); };

    UNIT_ASSERT_VALUES_EQUAL(
        TString(DatumGetCString(DirectFunctionCall1(numeric_out, NumericGetDatum(pgNumeric)))),
        longDyNumber);
}

} // Y_UNIT_TEST_SUITE(TPgBlockToPgTests)

#include <ydb/core/scheme/scheme_types_proto.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TypesProto) {

    Y_UNIT_TEST(LegacyPgMetadataRejected) {
        ::NKikimrProto::TTypeInfo typeInfoProto;
        UNIT_ASSERT(typeInfoProto.ParseFromString(TString("\x08\x17", 2)));

        UNIT_ASSERT_EXCEPTION(NScheme::TypeInfoFromProto(NScheme::NTypeIds::Pg, typeInfoProto), yexception);
        UNIT_ASSERT_EXCEPTION(NScheme::TypeInfoModFromProtoColumnType(NScheme::NTypeIds::Pg, &typeInfoProto), yexception);
    }

    Y_UNIT_TEST(DecimalNoTypeInfo) {
        ::NKikimrProto::TTypeInfo typeInfoProto;

        NScheme::TTypeInfo typeInfo = NKikimr::NScheme::TypeInfoFromProto(NScheme::NTypeIds::Decimal, typeInfoProto);

        UNIT_ASSERT(typeInfo == NScheme::TTypeInfo(NScheme::TDecimalType::Default()));
    }

    Y_UNIT_TEST(Decimal22) {
        ::NKikimrProto::TTypeInfo typeInfoProto;
        typeInfoProto.SetDecimalPrecision(NScheme::DECIMAL_PRECISION);
        typeInfoProto.SetDecimalScale(NScheme::DECIMAL_SCALE);

        NScheme::TTypeInfo typeInfo = NKikimr::NScheme::TypeInfoFromProto(NScheme::NTypeIds::Decimal, typeInfoProto);

        UNIT_ASSERT(typeInfo == NScheme::TTypeInfo(NScheme::TDecimalType::Default()));
    }

    Y_UNIT_TEST(Decimal35) {
        ::NKikimrProto::TTypeInfo typeInfoProto;
        typeInfoProto.SetDecimalPrecision(35);
        typeInfoProto.SetDecimalScale(10);

        NScheme::TTypeInfo typeInfo = NKikimr::NScheme::TypeInfoFromProto(NScheme::NTypeIds::Decimal, typeInfoProto);

        UNIT_ASSERT(typeInfo.GetDecimalType().GetPrecision() == 35);
        UNIT_ASSERT(typeInfo.GetDecimalType().GetScale() == 10);
    }

}

}

#include "presort.h"

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/hex.h>

using namespace std::literals::string_view_literals;

namespace NKikimr {
namespace NMiniKQL {

namespace {

#define TYPE_MAP(XX) \
    XX(bool, Bool) \
    XX(ui8, Uint8) \
    XX(ui16, Uint16) \
    XX(ui32, Uint32) \
    XX(ui64, Uint64) \
    XX(i8, Int8) \
    XX(i16, Int16) \
    XX(i32, Int32) \
    XX(i64, Int64) \
    XX(float, Float) \
    XX(double, Double)

#define ALL_VALUES(xType, xName) \
    xType xName;

struct TSimpleTypes {
    TYPE_MAP(ALL_VALUES)
};
#undef ALL_VALUES

#define ADD_TYPE(xType, xName) \
    codec.AddType(NUdf::EDataSlot::xName, isOptional, isDesc);

void AddTypes(TPresortCodec& codec, bool isOptional, bool isDesc) {
    TYPE_MAP(ADD_TYPE)
}
#undef ADD_TYPE

#define ENCODE(xType, xName) \
    encoder.Encode(NUdf::TUnboxedValuePod(values.xName));

TStringBuf Encode(NKikimr::NMiniKQL::TPresortEncoder& encoder, const TSimpleTypes& values) {
    encoder.Start();
    TYPE_MAP(ENCODE)
    return encoder.Finish();
}
#undef ENCODE

#define DECODE(xType, xName) \
    UNIT_ASSERT_EQUAL(decoder.Decode().Get<xType>(), values.xName);

void Decode(TPresortDecoder& decoder, TStringBuf input, const TSimpleTypes& values) {
    decoder.Start(input);
    TYPE_MAP(DECODE)
    decoder.Finish();
}
#undef DECODE

#undef TYPE_MAP

struct TPresortTest {
    TScopedAlloc Alloc;
    TMemoryUsageInfo MemInfo;

    TPresortTest()
        : Alloc(__LOCATION__)
        , MemInfo("Memory")
    {}

    template <typename T>
    void ValidateEncoding(bool isDesc, T value, const TString& hex) {
        TPresortEncoder encoder;
        encoder.AddType(NUdf::TDataType<T>::Slot, false, isDesc);

        TPresortDecoder decoder;
        decoder.AddType(NUdf::TDataType<T>::Slot, false, isDesc);

        encoder.Start();
        encoder.Encode(NUdf::TUnboxedValuePod(value));
        auto bytes = encoder.Finish();

        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(bytes.data(), bytes.size()), hex);

        decoder.Start(bytes);
        auto decoded = decoder.Decode().Get<T>();
        decoder.Finish();

        UNIT_ASSERT_EQUAL(decoded, value);
    };

    template <NUdf::EDataSlot Slot>
    void ValidateEncoding(bool isDesc, TStringBuf value, const TString& hex) {
        TPresortEncoder encoder;
        encoder.AddType(Slot, false, isDesc);

        TPresortDecoder decoder;
        decoder.AddType(Slot, false, isDesc);

        encoder.Start();
        encoder.Encode(NUdf::TUnboxedValue(MakeString(NUdf::TStringRef(value))));
        auto bytes = encoder.Finish();

        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(bytes.data(), bytes.size()), hex);

        decoder.Start(bytes);
        auto uv = decoder.Decode();
        decoder.Finish();

        auto stringRef = uv.AsStringRef();
        auto decoded = TStringBuf(stringRef.Data(), stringRef.Size());

        UNIT_ASSERT_EQUAL(decoded, value);
    }

    template <NUdf::EDataSlot Slot, typename T>
    void ValidateEncoding(bool isDesc, const std::pair<T, ui16>& value, const TString& hex) {
        TPresortEncoder encoder;
        encoder.AddType(Slot, false, isDesc);

        TPresortDecoder decoder;
        decoder.AddType(Slot, false, isDesc);

        NUdf::TUnboxedValuePod uv(value.first);
        uv.SetTimezoneId(value.second);

        encoder.Start();
        encoder.Encode(uv);
        auto bytes = encoder.Finish();

        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(bytes.data(), bytes.size()), hex);

        decoder.Start(bytes);
        auto decoded = decoder.Decode();
        decoder.Finish();

        UNIT_ASSERT_EQUAL(decoded.Get<T>(), value.first);
        UNIT_ASSERT_EQUAL(decoded.GetTimezoneId(), value.second);
    };

    void ValidateEncoding(bool isDesc, NYql::NDecimal::TInt128 value, const TString& hex) {
        TPresortEncoder encoder;
        encoder.AddType(NUdf::EDataSlot::Decimal, false, isDesc);

        TPresortDecoder decoder;
        decoder.AddType(NUdf::EDataSlot::Decimal, false, isDesc);

        encoder.Start();
        encoder.Encode(NUdf::TUnboxedValuePod(value));
        auto bytes = encoder.Finish();

        UNIT_ASSERT_STRINGS_EQUAL(HexEncode(bytes.data(), bytes.size()), hex);

        decoder.Start(bytes);
        auto decoded = decoder.Decode().GetInt128();
        decoder.Finish();

        UNIT_ASSERT_EQUAL(decoded, value);
    };

    template <typename T>
    void ValidateEncoding(const TVector<T>& values) {
        for (auto& value : values) {
            ValidateEncoding(false, std::get<0>(value), std::get<1>(value));
            ValidateEncoding(true, std::get<0>(value), std::get<2>(value));
        }
    }

    template <NUdf::EDataSlot Slot, typename T>
    void ValidateEncoding(const TVector<T>& values) {
        for (auto& value : values) {
            ValidateEncoding<Slot>(false, std::get<0>(value), std::get<1>(value));
            ValidateEncoding<Slot>(true, std::get<0>(value), std::get<2>(value));
        }
    }
};

}

Y_UNIT_TEST_SUITE(TPresortCodecTest) {

Y_UNIT_TEST(SimpleTypes) {
    TPresortTest test;

    TSimpleTypes values = {false, 1u, 2u, 3u, 4u, 5, 6, 7, 8, 9.f, 10.0};

    auto validateSimpleTypes = [&] (bool isOptional, bool isDesc) {
        TPresortEncoder encoder;
        AddTypes(encoder, isOptional, isDesc);

        TPresortDecoder decoder;
        AddTypes(decoder, isOptional, isDesc);

        auto bytes = Encode(encoder, values);
        Decode(decoder, bytes, values);
    };

    validateSimpleTypes(false, false);
    validateSimpleTypes(false, true);
    validateSimpleTypes(true, false);
    validateSimpleTypes(true, true);
}


Y_UNIT_TEST(Bool) {
    const TVector<std::tuple<bool, TString, TString>> values = {
        {false, "00", "FF"},
        {true, "01", "FE"}
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Int8) {
    const TVector<std::tuple<i8, TString, TString>> values = {
        {-0x80, "00", "FF"},
        {-1,    "7F", "80"},
        {0,     "80", "7F"},
        {1,     "81", "7E"},
        {0x7F,  "FF", "00"}
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Uint8) {
    const TVector<std::tuple<ui8, TString, TString>> values = {
        {0u,    "00", "FF"},
        {0x80u, "80", "7F"},
        {0xFFu, "FF", "00"},
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Int16) {
    const TVector<std::tuple<i16, TString, TString>> values = {
        {-0x8000, "0000", "FFFF"},
        {-1,      "7FFF", "8000"},
        {0,       "8000", "7FFF"},
        {1,       "8001", "7FFE"},
        {0x7FFF,  "FFFF", "0000"}
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Uint16) {
    const TVector<std::tuple<ui16, TString, TString>> values = {
        {0,       "0000", "FFFF"},
        {0x8000u, "8000", "7FFF"},
        {0xFFFFu, "FFFF", "0000"},
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Int32) {
    const TVector<std::tuple<i32, TString, TString>> values = {
        {-0x80000000, "00000000", "FFFFFFFF"},
        {-1,          "7FFFFFFF", "80000000"},
        {0,           "80000000", "7FFFFFFF"},
        {1,           "80000001", "7FFFFFFE"},
        {0x7FFFFFFF,  "FFFFFFFF", "00000000"}
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Uint32) {
    const TVector<std::tuple<ui32, TString, TString>> values = {
        {0u,          "00000000", "FFFFFFFF"},
        {0x80000000u, "80000000", "7FFFFFFF"},
        {0xFFFFFFFFu, "FFFFFFFF", "00000000"},
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Int64) {
    const TVector<std::tuple<i64, TString, TString>> values = {
        {-0x8000000000000000, "0000000000000000", "FFFFFFFFFFFFFFFF"},
        {-1,                  "7FFFFFFFFFFFFFFF", "8000000000000000"},
        {0,                   "8000000000000000", "7FFFFFFFFFFFFFFF"},
        {1,                   "8000000000000001", "7FFFFFFFFFFFFFFE"},
        {0x7FFFFFFFFFFFFFFF,  "FFFFFFFFFFFFFFFF", "0000000000000000"}
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Uint64) {
    const TVector<std::tuple<ui64, TString, TString>> values = {
        {0u,                  "0000000000000000", "FFFFFFFFFFFFFFFF"},
        {0x8000000000000000u, "8000000000000000", "7FFFFFFFFFFFFFFF"},
        {0xFFFFFFFFFFFFFFFFu, "FFFFFFFFFFFFFFFF", "0000000000000000"}
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Float) {
    using TLimits = std::numeric_limits<float>;

    const TVector<std::tuple<float, TString, TString>> values = {
        {-TLimits::infinity(), "00", "FF"},
        {-TLimits::max(),      "0100800000", "FEFF7FFFFF"},
        {-1.f,                 "01407FFFFF", "FEBF800000"},
        {-TLimits::min(),      "017F7FFFFF", "FE80800000"},
        {-TLimits::min()/8.f,  "017FEFFFFF", "FE80100000"},
        {0.f,                  "02", "FD"},
        {TLimits::min()/8.f,   "0300100000", "FCFFEFFFFF"},
        {TLimits::min(),       "0300800000", "FCFF7FFFFF"},
        {1.f,                  "033F800000", "FCC07FFFFF"},
        {TLimits::max(),       "037F7FFFFF", "FC80800000"},
        {TLimits::infinity(),  "04", "FB"},
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(Double) {
    using TLimits = std::numeric_limits<double>;

    const TVector<std::tuple<double, TString, TString>> values = {
        {-TLimits::infinity(), "00", "FF"},
        {-TLimits::max(),      "010010000000000000", "FEFFEFFFFFFFFFFFFF"},
        {-1.,                  "01400FFFFFFFFFFFFF", "FEBFF0000000000000"},
        {-TLimits::min(),      "017FEFFFFFFFFFFFFF", "FE8010000000000000"},
        {-TLimits::min()/8.,   "017FFDFFFFFFFFFFFF", "FE8002000000000000"},
        {0.,                   "02", "FD"},
        {TLimits::min()/8.,    "030002000000000000", "FCFFFDFFFFFFFFFFFF"},
        {TLimits::min(),       "030010000000000000", "FCFFEFFFFFFFFFFFFF"},
        {1.,                   "033FF0000000000000", "FCC00FFFFFFFFFFFFF"},
        {TLimits::max(),       "037FEFFFFFFFFFFFFF", "FC8010000000000000"},
        {TLimits::infinity(),  "04", "FB"},
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(String) {
    const TVector<std::tuple<TStringBuf, TString, TString>> values = {
        {TStringBuf(""), "00", "FF"},
        {"\x00"sv, "1F00000000000000000000000000000001",
            "E0FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE"},
        {"\x01", "1F01000000000000000000000000000001",
            "E0FEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE"},
        {"0", "1F30000000000000000000000000000001",
            "E0CFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE"},
        {"0123", "1F30313233000000000000000000000004",
            "E0CFCECDCCFFFFFFFFFFFFFFFFFFFFFFFB"},
        {"0123456789abcde", "1F3031323334353637383961626364650F",
            "E0CFCECDCCCBCAC9C8C7C69E9D9C9B9AF0"},
        {"a", "1F61000000000000000000000000000001",
            "E09EFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE"},
        {"a\x00"sv, "1F61000000000000000000000000000002",
            "E09EFFFFFFFFFFFFFFFFFFFFFFFFFFFFFD"},
        {"abc", "1F61626300000000000000000000000003",
            "E09E9D9CFFFFFFFFFFFFFFFFFFFFFFFFFC"},
        {"b", "1F62000000000000000000000000000001",
            "E09DFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::String>(values);
}

Y_UNIT_TEST(Uuid) {
    const TVector<std::tuple<TStringBuf, TString, TString>> values = {
        {"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0"sv,
            "00000000000000000000000000000000",
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"},
        {"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1"sv,
            "00000000000000000000000000000001",
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFE"},
        {"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
            "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
            "00000000000000000000000000000000"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::Uuid>(values);
}

Y_UNIT_TEST(TzDate) {
    const TVector<std::tuple<std::pair<ui16, ui16>, TString, TString>> values = {
        {{0u, 0u}, "00000000", "FFFFFFFF"},
        {{0u, 1u}, "00000001", "FFFFFFFE"},
        {{1u, 0u}, "00010000", "FFFEFFFF"},
        {{NUdf::MAX_DATE, 0u}, "C2090000", "3DF6FFFF"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::TzDate>(values);
}

Y_UNIT_TEST(TzDatetime) {
    const TVector<std::tuple<std::pair<ui32, ui16>, TString, TString>> values = {
        {{0u, 0u}, "000000000000", "FFFFFFFFFFFF"},
        {{0u, 1u}, "000000000001", "FFFFFFFFFFFE"},
        {{1u, 0u}, "000000010000", "FFFFFFFEFFFF"},
        {{NUdf::MAX_DATETIME, 0u}, "FFCEDD800000", "0031227FFFFF"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::TzDatetime>(values);
}

Y_UNIT_TEST(TzTimestamp) {
    const TVector<std::tuple<std::pair<ui64, ui16>, TString, TString>> values = {
        {{0u, 0u}, "00000000000000000000", "FFFFFFFFFFFFFFFFFFFF"},
        {{0u, 1u}, "00000000000000000001", "FFFFFFFFFFFFFFFFFFFE"},
        {{1u, 0u}, "00000000000000010000", "FFFFFFFFFFFFFFFEFFFF"},
        {{NUdf::MAX_TIMESTAMP, 0u}, "000F3F52435260000000", "FFF0C0ADBCAD9FFFFFFF"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::TzTimestamp>(values);
}

Y_UNIT_TEST(TzDate32) {
    const TVector<std::tuple<std::pair<i32, ui16>, TString, TString>> values = {
        {{0, 0u}, "800000000000", "7FFFFFFFFFFF"},
        {{0, 1u}, "800000000001", "7FFFFFFFFFFE"},
        {{1, 0u}, "800000010000", "7FFFFFFEFFFF"},
        {{NUdf::MIN_DATE32, 0u}, "7CD18CBF0000", "832E7340FFFF"},
        {{NUdf::MAX_DATE32, 0u}, "832E733F0000", "7CD18CC0FFFF"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::TzDate32>(values);
}

Y_UNIT_TEST(TzDatetime64) {
    const TVector<std::tuple<std::pair<i64, ui16>, TString, TString>> values = {
        {{0, 0u}, "80000000000000000000", "7FFFFFFFFFFFFFFFFFFF"},
        {{0, 1u}, "80000000000000000001", "7FFFFFFFFFFFFFFFFFFE"},
        {{1, 0u}, "80000000000000010000", "7FFFFFFFFFFFFFFEFFFF"},
        {{NUdf::MIN_DATETIME64, 0u}, "7FFFFBCE430DCE800000", "80000431BCF2317FFFFF"},
        {{NUdf::MAX_DATETIME64, 0u}, "80000431BCF0DFFF0000", "7FFFFBCE430F2000FFFF"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::TzDatetime64>(values);
}

Y_UNIT_TEST(TzTimestamp64) {
    const TVector<std::tuple<std::pair<i64, ui16>, TString, TString>> values = {
        {{0, 0u}, "80000000000000000000", "7FFFFFFFFFFFFFFFFFFF"},
        {{0, 1u}, "80000000000000000001", "7FFFFFFFFFFFFFFFFFFE"},
        {{1, 0u}, "80000000000000010000", "7FFFFFFFFFFFFFFEFFFF"},
        {{NUdf::MIN_TIMESTAMP64, 0u}, "40000EA96C30A0000000", "BFFFF15693CF5FFFFFFF"},
        {{NUdf::MAX_TIMESTAMP64, 0u}, "BFFFF14275F7FFFF0000", "40000EBD8A080000FFFF"},
    };
    TPresortTest().ValidateEncoding<NUdf::EDataSlot::TzTimestamp64>(values);
}

Y_UNIT_TEST(Decimal) {
    const TVector<std::tuple<NYql::NDecimal::TInt128, TString, TString>> values = {
        {-NYql::NDecimal::Nan(),
            "00",
            "FF"},
        {-NYql::NDecimal::Inf(),
            "01",
            "FE"},
        {NYql::NDecimal::TInt128(-1),
            "7F",
            "8101"},
        {NYql::NDecimal::TInt128(0),
            "80",
            "80"},
        {NYql::NDecimal::TInt128(1),
            "8101",
            "7F"},
        {NYql::NDecimal::Inf(),
            "FE",
            "01"},
        {NYql::NDecimal::Nan(),
            "FF",
            "00"},
    };
    TPresortTest().ValidateEncoding(values);
}

Y_UNIT_TEST(GenericVoid) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto type = env.GetVoidLazy()->GetType();
    NUdf::TUnboxedValue value = NUdf::TUnboxedValuePod::Void();
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf(""));
}

Y_UNIT_TEST(GenericBool) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto type = TDataType::Create(NUdf::TDataType<bool>::Id, env);
    NUdf::TUnboxedValue value = NUdf::TUnboxedValuePod(true);
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01"));
    buf = encoder.Encode(value, true);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\xFE"));
}

Y_UNIT_TEST(GenericNumber) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto type = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
    NUdf::TUnboxedValue value = NUdf::TUnboxedValuePod(ui32(1234));
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x00\x00\x04\xD2"sv));
}

Y_UNIT_TEST(GenericString) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto type = TDataType::Create(NUdf::TDataType<char*>::Id, env);
    NUdf::TUnboxedValue value = MakeString("ALongStringExample");
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x1F" "ALongStringExam\x1Fple\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x03"sv));
}

Y_UNIT_TEST(GenericOptional) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto type = TOptionalType::Create(TDataType::Create(NUdf::TDataType<bool>::Id, env), env);
    NUdf::TUnboxedValue value = NUdf::TUnboxedValuePod(true);
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x01"));
    value = {};
    buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x00"sv));
}

Y_UNIT_TEST(NestedOptional) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    // Int32???
    auto type =
        TOptionalType::Create(TOptionalType::Create(TOptionalType::Create(TDataType::Create(NUdf::TDataType<i32>::Id, env), env), env), env);
    TGenericPresortEncoder encoder(type);

    NUdf::TUnboxedValue null = {};
    auto buf = encoder.Encode(null, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x00"sv));

    auto justNull = null.MakeOptional();
    buf = encoder.Encode(justNull, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x00"sv));

    auto justJustNull = justNull.MakeOptional();
    buf = encoder.Encode(justJustNull, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x01\x00"sv));


    auto zero = NUdf::TUnboxedValuePod(0).MakeOptional().MakeOptional().MakeOptional();
    buf = encoder.Encode(zero, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x01\x01\x80\x00\x00\x00"sv));
}


Y_UNIT_TEST(GenericList) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto type = TListType::Create(TDataType::Create(NUdf::TDataType<bool>::Id, env), env);
    TMemoryUsageInfo memInfo("test");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    auto value = holderFactory.GetEmptyContainerLazy();
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x00"sv));
    NUdf::TUnboxedValue* items;
    value = holderFactory.CreateDirectArrayHolder(1, items);
    items[0] = NUdf::TUnboxedValuePod(true);
    buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x01\x00"sv));
    value = holderFactory.CreateDirectArrayHolder(2, items);
    items[0] = NUdf::TUnboxedValuePod(true);
    items[1] = NUdf::TUnboxedValuePod(false);
    buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x01\x01\x00\x00"sv));
}

Y_UNIT_TEST(GenericTuple) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TType* tupleTypes[2];
    tupleTypes[0] = TDataType::Create(NUdf::TDataType<bool>::Id, env);
    tupleTypes[1] = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
    auto type = TTupleType::Create(2, tupleTypes, env);
    TMemoryUsageInfo memInfo("test");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    NUdf::TUnboxedValue* items;
    auto value = holderFactory.CreateDirectArrayHolder(2, items);
    items[0] = NUdf::TUnboxedValuePod(true);
    items[1] = NUdf::TUnboxedValuePod(ui32(1234));
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x00\x00\x04\xD2"sv));
}

Y_UNIT_TEST(GenericStruct) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TStructMember structTypes[2];
    structTypes[0] = TStructMember("A", TDataType::Create(NUdf::TDataType<bool>::Id, env));
    structTypes[1] = TStructMember("B", TDataType::Create(NUdf::TDataType<ui32>::Id, env));
    auto type = TStructType::Create(2, structTypes, env);
    TMemoryUsageInfo memInfo("test");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    NUdf::TUnboxedValue* items;
    auto value = holderFactory.CreateDirectArrayHolder(2, items);
    items[0] = NUdf::TUnboxedValuePod(true);
    items[1] = NUdf::TUnboxedValuePod(ui32(1234));
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x00\x00\x04\xD2"sv));
}

Y_UNIT_TEST(GenericTupleVariant) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TType* tupleTypes[2];
    tupleTypes[0] = TDataType::Create(NUdf::TDataType<bool>::Id, env);
    tupleTypes[1] = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
    auto underlying = TTupleType::Create(2, tupleTypes, env);
    auto type = TVariantType::Create(underlying, env);
    TMemoryUsageInfo memInfo("test");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    TGenericPresortEncoder encoder(type);
    auto value = holderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(true), 0);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x00\x01"sv));
    value = holderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(ui32(1234)), 1);
    buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x00\x00\x04\xD2"sv));
}

Y_UNIT_TEST(GenericStructVariant) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TStructMember structTypes[2];
    structTypes[0] = TStructMember("A", TDataType::Create(NUdf::TDataType<bool>::Id, env));
    structTypes[1] = TStructMember("B", TDataType::Create(NUdf::TDataType<ui32>::Id, env));
    auto underlying = TStructType::Create(2, structTypes, env);
    auto type = TVariantType::Create(underlying, env);
    TMemoryUsageInfo memInfo("test");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    TGenericPresortEncoder encoder(type);
    auto value = holderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(true), 0);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x00\x01"sv));
    value = holderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(ui32(1234)), 1);
    buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x00\x00\x04\xD2"sv));
}

Y_UNIT_TEST(GenericDict) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    auto type = TDictType::Create(TDataType::Create(NUdf::TDataType<ui32>::Id, env),
        TDataType::Create(NUdf::TDataType<bool>::Id, env), env);
    TKeyTypes keyTypes;
    bool isTuple;
    bool encoded;
    bool useIHash;
    GetDictionaryKeyTypes(type->GetKeyType(), keyTypes, isTuple, encoded, useIHash);
    UNIT_ASSERT(!isTuple);
    UNIT_ASSERT(!encoded);
    UNIT_ASSERT(!useIHash);
    TMemoryUsageInfo memInfo("test");
    THolderFactory holderFactory(alloc.Ref(), memInfo);
    auto value = holderFactory.GetEmptyContainerLazy();
    TGenericPresortEncoder encoder(type);
    auto buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x00"sv));
    value = holderFactory.CreateDirectHashedDictHolder([](TValuesDictHashMap& map) {
        map.emplace(NUdf::TUnboxedValuePod(ui32(1234)), NUdf::TUnboxedValuePod(true));
    }, keyTypes, false, true, nullptr, nullptr, nullptr);
    buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x00\x00\x04\xD2\x01\x00"sv));
    value = holderFactory.CreateDirectHashedDictHolder([](TValuesDictHashMap& map) {
        map.emplace(NUdf::TUnboxedValuePod(ui32(5678)), NUdf::TUnboxedValuePod(false));
        map.emplace(NUdf::TUnboxedValuePod(ui32(1234)), NUdf::TUnboxedValuePod(true));
    }, keyTypes, false, true, nullptr, nullptr, nullptr);
    buf = encoder.Encode(value, false);
    UNIT_ASSERT_NO_DIFF(buf, TStringBuf("\x01\x00\x00\x04\xD2\x01\x01\x00\x00\x16\x2E\x00\x00"sv));
}

}

} // NMiniKQL
} // NKikimr

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/hex.h>

#include <ydb/core/kqp/common/result_set_format/ut/kqp_formats_ut_helpers.h>
#include <ydb/core/kqp/common/result_set_format/kqp_formats_arrow.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <yql/essentials/minikql/computation/mkql_value_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/udf/arrow/defs.h>
#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <library/cpp/type_info/tz/tz.h>

using namespace NKikimr::NMiniKQL;
using namespace NYql;

inline static constexpr size_t TEST_ARRAY_DATATYPE_SIZE = 1 << 16;
inline static constexpr size_t TEST_ARRAY_NESTED_SIZE = 1 << 8;
inline static constexpr ui8 DECIMAL_PRECISION = 35;
inline static constexpr ui8 DECIMAL_SCALE = 10;

static_assert(DECIMAL_PRECISION >= DECIMAL_SCALE, "Decimal precision must be greater than or equal to scale");

namespace {

ui16 GetTimezoneIdSkipEmpty(ui16 index) {
    auto size = NTi::GetTimezones().size();
    while (NTi::GetTimezones()[index % size].empty()) {
        index = (index + 1) % size;
    }
    return GetTimezoneId(NTi::GetTimezones()[index % size]);
}

std::string SerializeToBinaryJson(const TStringBuf json) {
    auto variant = NKikimr::NBinaryJson::SerializeToBinaryJson(json);
    if (std::holds_alternative<NKikimr::NBinaryJson::TBinaryJson>(variant)) {
        const auto binaryJson = std::get<NKikimr::NBinaryJson::TBinaryJson>(variant);
        return std::string(binaryJson.Data(), binaryJson.Size());
    }
    UNIT_ASSERT_C(false, "Cannot serialize binary json");
    return {};
}

NUdf::TUnboxedValue GetValueOfBasicType(TType* type, ui64 value) {
    Y_ABORT_UNLESS(type->GetKind() == TType::EKind::Data);
    auto dataType = static_cast<const TDataType*>(type);
    auto slot = *dataType->GetDataSlot().Get();
    switch (slot) {
        case NUdf::EDataSlot::Bool:
            return NUdf::TUnboxedValuePod(static_cast<bool>(value % 2 == 0));
        case NUdf::EDataSlot::Int8:
            return NUdf::TUnboxedValuePod(static_cast<i8>(-(value % ((1 << 7) - 1))));
        case NUdf::EDataSlot::Uint8:
            return NUdf::TUnboxedValuePod(static_cast<ui8>(value % ((1 << 8))));
        case NUdf::EDataSlot::Int16:
            return NUdf::TUnboxedValuePod(static_cast<i16>(-(value % ((1 << 15) - 1))));
        case NUdf::EDataSlot::Uint16:
            return NUdf::TUnboxedValuePod(static_cast<ui16>(value % (1 << 15)));
        case NUdf::EDataSlot::Int32:
            return NUdf::TUnboxedValuePod(static_cast<i32>(-(value % ((1ULL << 31) - 1))));
        case NUdf::EDataSlot::Uint32:
            return NUdf::TUnboxedValuePod(static_cast<ui32>(value % (1ULL << 31)));
        case NUdf::EDataSlot::Int64:
            return NUdf::TUnboxedValuePod(static_cast<i64>(-(value % ((1ULL << 63) - 1))));
        case NUdf::EDataSlot::Uint64:
            return NUdf::TUnboxedValuePod(static_cast<ui64>(value % (1ULL << 63)));
        case NUdf::EDataSlot::Float:
            return NUdf::TUnboxedValuePod(static_cast<float>(value) / 1234);
        case NUdf::EDataSlot::Double:
            return NUdf::TUnboxedValuePod(static_cast<double>(value) / 12345);
        case NUdf::EDataSlot::Decimal: {
            auto decimal = NDecimal::FromString(TStringBuilder() << value << ".123", DECIMAL_PRECISION, DECIMAL_SCALE);
            return NUdf::TUnboxedValuePod(decimal);
        }
        case NUdf::EDataSlot::DyNumber: {
            auto number = NKikimr::NDyNumber::ParseDyNumberString(TStringBuilder() << value);
            UNIT_ASSERT_C(number.Defined(), "Failed to convert string to DyNumber");
            return MakeString(*number);
        }
        case NUdf::EDataSlot::Date:
            return NUdf::TUnboxedValuePod(static_cast<ui16>(value % NUdf::MAX_DATE));
        case NUdf::EDataSlot::Datetime:
            return NUdf::TUnboxedValuePod(static_cast<ui32>(value % NUdf::MAX_DATETIME));
        case NUdf::EDataSlot::Timestamp:
            return NUdf::TUnboxedValuePod(static_cast<ui64>(value % NUdf::MAX_TIMESTAMP));
        case NUdf::EDataSlot::Interval:
            return NUdf::TUnboxedValuePod(static_cast<i64>(value / 2 - 1));
        case NUdf::EDataSlot::TzDate: {
            auto ret = NUdf::TUnboxedValuePod(static_cast<ui16>(value % NUdf::MAX_DATE));
            ret.SetTimezoneId(GetTimezoneIdSkipEmpty(value));
            return ret;
        }
        case NUdf::EDataSlot::TzDatetime: {
            auto ret = NUdf::TUnboxedValuePod(static_cast<ui32>(value % NUdf::MAX_DATETIME));
            ret.SetTimezoneId(GetTimezoneIdSkipEmpty(value));
            return ret;
        }
        case NUdf::EDataSlot::TzTimestamp: {
            auto ret = NUdf::TUnboxedValuePod(static_cast<ui64>(value % NUdf::MAX_TIMESTAMP));
            ret.SetTimezoneId(GetTimezoneIdSkipEmpty(value));
            return ret;
        }
        case NUdf::EDataSlot::Date32:
            return NUdf::TUnboxedValuePod(static_cast<i32>(value % NUdf::MAX_DATE32));
        case NUdf::EDataSlot::Datetime64:
            return NUdf::TUnboxedValuePod(static_cast<i64>(value % NUdf::MAX_DATETIME64));
        case NUdf::EDataSlot::Timestamp64:
            return NUdf::TUnboxedValuePod(static_cast<i64>(value % NUdf::MAX_TIMESTAMP64));
        case NUdf::EDataSlot::Interval64:
            return NUdf::TUnboxedValuePod(static_cast<i64>(value % NUdf::MAX_INTERVAL64));
        case NUdf::EDataSlot::TzDate32: {
            auto ret = NUdf::TUnboxedValuePod(static_cast<i32>(value % NUdf::MAX_DATE32));
            ret.SetTimezoneId(GetTimezoneIdSkipEmpty(value));
            return ret;
        }
        case NUdf::EDataSlot::TzDatetime64: {
            auto ret = NUdf::TUnboxedValuePod(static_cast<i64>(value % NUdf::MAX_DATETIME64));
            ret.SetTimezoneId(GetTimezoneIdSkipEmpty(value));
            return ret;
        }
        case NUdf::EDataSlot::TzTimestamp64: {
            auto ret = NUdf::TUnboxedValuePod(static_cast<i64>(value % NUdf::MAX_TIMESTAMP64));
            ret.SetTimezoneId(GetTimezoneIdSkipEmpty(value));
            return ret;
        }
        case NUdf::EDataSlot::String: {
            std::string string = TStringBuilder() << value;
            return MakeString(NUdf::TStringRef(string.data(), string.size()));
        }
        case NUdf::EDataSlot::Utf8: {
            std::string string = TStringBuilder() << value << "utf8";
            return MakeString(NUdf::TStringRef(string.data(), string.size()));
        }
        case NUdf::EDataSlot::Yson: {
            std::string yson = TStringBuilder() << '[' << value << ']';
            return MakeString(NUdf::TStringRef(yson.data(), yson.size()));
        }
        case NUdf::EDataSlot::Json: {
            std::string json = TStringBuilder() << '[' << value << ']';
            return MakeString(NUdf::TStringRef(json.data(), json.size()));
        }
        case NUdf::EDataSlot::JsonDocument: {
            std::string json = SerializeToBinaryJson(TStringBuilder() << "{\"b\": " << value << ", \"a\": " << value / 2 << "}");
            return MakeString(NUdf::TStringRef(json.data(), json.size()));
        }
        case NUdf::EDataSlot::Uuid: {
            std::string uuid;
            for (size_t i = 0; i < NKikimr::NScheme::FSB_SIZE / 2; ++i) {
                uuid += "a" + std::to_string((i + value) % 10);
            }
            return MakeString(NUdf::TStringRef(uuid));
        }
    }

    return NUdf::TUnboxedValuePod();
}

struct TTestContext {
    TScopedAlloc Alloc;
    TTypeEnvironment TypeEnv;
    TMemoryUsageInfo MemInfo;
    THolderFactory HolderFactory;
    TDefaultValueBuilder Vb;
    ui16 VariantSize = 0;

    TVector<TType*> BasicTypes = {
        TDataType::Create(NUdf::TDataType<bool>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i8>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui8>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i16>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui16>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui32>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<i64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<float>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<double>::Id, TypeEnv),
        TDataDecimalType::Create(DECIMAL_PRECISION, DECIMAL_SCALE, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TDyNumber>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TDate>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TDatetime>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTimestamp>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TInterval>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTzDate>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTzDatetime>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTzTimestamp>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TDate32>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TDatetime64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTimestamp64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TInterval64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTzDate32>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTzDatetime64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TTzTimestamp64>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<char*>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TYson>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TJson>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TJsonDocument>::Id, TypeEnv),
        TDataType::Create(NUdf::TDataType<NUdf::TUuid>::Id, TypeEnv)
    };

    TTestContext()
        : Alloc(__LOCATION__)
        , TypeEnv(Alloc)
        , MemInfo("TestMem")
        , HolderFactory(Alloc.Ref(), MemInfo)
        , Vb(HolderFactory)
    {
    }

    TType* GetStructType() {
        std::vector<TStructMember> members = {
            {"ABC", TDataType::Create(NUdf::TDataType<char*>::Id, TypeEnv)},
            {"DEF", TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv)},
            {"GHI", TDataType::Create(NUdf::TDataType<ui64>::Id, TypeEnv)},
            {"JKL", TDataType::Create(NUdf::TDataType<NUdf::TInterval>::Id, TypeEnv)},
            {"MNO", TDataType::Create(NUdf::TDataType<NUdf::TUtf8>::Id, TypeEnv)},
        };
        return TStructType::Create(5, members.data(), TypeEnv);
    }

    TUnboxedValueVector CreateStructs(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui32 value = 0; value < quantity; ++value) {
            NUdf::TUnboxedValue* items;
            auto structValue = Vb.NewArray(5, items);

            std::string string = TStringBuilder() << value;
            items[0] = MakeString(NUdf::TStringRef(string.data(), string.size()));
            items[1] = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
            items[2] = NUdf::TUnboxedValuePod((ui64) (value));
            items[3] = NUdf::TUnboxedValuePod(static_cast<i64>(-value));
            items[4] = NUdf::TUnboxedValuePod(MakeString(NUdf::TStringRef(string.data(), string.size())));

            values.emplace_back(std::move(structValue));
        }
        return values;
    }

    TType* GetTupleType() {
        TType* members[3] = {
            TDataType::Create(NUdf::TDataType<bool>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<i8>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<ui8>::Id, TypeEnv)
        };
        return TTupleType::Create(3, members, TypeEnv);
    }

    TUnboxedValueVector CreateTuples(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui32 value = 0; value < quantity; ++value) {
            NUdf::TUnboxedValue* items;
            auto tupleValue = Vb.NewArray(3, items);
            items[0] = NUdf::TUnboxedValuePod(value % 3 == 0);
            items[1] = NUdf::TUnboxedValuePod(static_cast<i8>(-value));
            items[2] = NUdf::TUnboxedValuePod(static_cast<ui8>(value));
            values.push_back(std::move(tupleValue));
        }
        return values;
    }

    TType* GetListType() {
        auto itemType = TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv);
        return TListType::Create(itemType, TypeEnv);
    }

    TUnboxedValueVector CreateLists(ui32 quantity) {
        TUnboxedValueVector values;
        values.reserve(quantity);
        for (ui64 value = 0; value < quantity; ++value) {
            TUnboxedValueVector items;
            items.reserve(value);
            for (ui64 i = 0; i < value; ++i) {
                items.push_back(NUdf::TUnboxedValuePod(static_cast<i32>(-i)));
            }
            auto listValue = Vb.NewList(items.data(), value);
            values.emplace_back(std::move(listValue));
        }
        return values;
    }

    TType* GetOptionalListOfOptional() {
        TType* itemType = TOptionalType::Create(TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv), TypeEnv);
        return TOptionalType::Create(TListType::Create(itemType, TypeEnv), TypeEnv);
    }

    TUnboxedValueVector CreateOptionalListOfOptional(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            if (value % 2 == 0) {
                values.emplace_back(NUdf::TUnboxedValuePod());
                continue;
            }

            TUnboxedValueVector items;
            items.reserve(value);
            for (ui64 i = 0; i < value; ++i) {
                NUdf::TUnboxedValue item = ((value + i) % 2 == 0) ? NUdf::TUnboxedValuePod() : NUdf::TUnboxedValuePod(i);
                items.push_back(std::move(item).MakeOptional());
            }

            auto listValue = Vb.NewList(items.data(), value);
            values.emplace_back(std::move(listValue).MakeOptional());
        }
        return values;
    }

    TType* GetVariantOverStructType() {
        TStructMember members[4] = {
            {"0_yson", TDataType::Create(NUdf::TDataType<NUdf::TYson>::Id, TypeEnv)},
            {"1_json-document", TDataType::Create(NUdf::TDataType<NUdf::TJsonDocument>::Id, TypeEnv)},
            {"2_uuid", TDataType::Create(NUdf::TDataType<NUdf::TUuid>::Id, TypeEnv)},
            {"3_float", TDataType::Create(NUdf::TDataType<float>::Id, TypeEnv)}
        };
        auto structType = TStructType::Create(4, members, TypeEnv);
        return TVariantType::Create(structType, TypeEnv);
    }

    TUnboxedValueVector CreateVariantOverStruct(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto typeIndex = value % 4;
            NUdf::TUnboxedValue item;
            if (typeIndex == 0) {
                std::string data = TStringBuilder() << "{value=" << value << "}";
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
            } else if (typeIndex == 1) {
                std::string data = TStringBuilder() << "{\"value\":" << value << "}";
                item = MakeString(SerializeToBinaryJson(data));
            } else if (typeIndex == 2) {
                std::string sample = "7856341212905634789012345678901";
                std::string data = TStringBuilder() << HexDecode(sample + static_cast<char>('0' + (value % 10)));
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
            } else if (typeIndex == 3) {
                item = NUdf::TUnboxedValuePod(static_cast<float>(value) / 4);
            }
            auto wrapped = Vb.NewVariant(typeIndex, std::move(item));
            values.push_back(std::move(wrapped));
        }
        return values;
    }

    TType* GetOptionalVariantOverStructType() {
        return TOptionalType::Create(GetVariantOverStructType(), TypeEnv);
    }

    TUnboxedValueVector CreateOptionalVariantOverStruct(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto typeIndex = value % 4;
            NUdf::TUnboxedValue item;

            if (value % 2 == 0) {
                values.push_back(NUdf::TUnboxedValuePod());
                continue;
            }

            if (typeIndex == 0) {
                std::string data = TStringBuilder() << "{value=" << value << "}";
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
            } else if (typeIndex == 1) {
                std::string data = TStringBuilder() << "{\"value\":" << value << "}";
                item = MakeString(SerializeToBinaryJson(data));
            } else if (typeIndex == 2) {
                std::string sample = "7856341212905634789012345678901";
                std::string data = TStringBuilder() << HexDecode(sample + static_cast<char>('0' + (value % 10)));
                item = MakeString(NUdf::TStringRef(data.data(), data.size()));
            } else if (typeIndex == 3) {
                item = NUdf::TUnboxedValuePod(static_cast<float>(value) / 4);
            }
            auto wrapped = Vb.NewVariant(typeIndex, std::move(item)).MakeOptional();
            values.push_back(std::move(wrapped));
        }
        return values;
    }

    TType* GetDoubleOptionalVariantOverStructType() {
        return TOptionalType::Create(GetOptionalVariantOverStructType(), TypeEnv);
    }

    TUnboxedValueVector CreateDoubleOptionalVariantOverStruct(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto typeIndex = value % 4;
            NUdf::TUnboxedValue item;

            if (value % 3 == 0) {
                if (typeIndex == 0) {
                    std::string data = TStringBuilder() << "{value=" << value << "}";
                    item = MakeString(NUdf::TStringRef(data.data(), data.size()));
                } else if (typeIndex == 1) {
                    std::string data = TStringBuilder() << "{\"value\":" << value << "}";
                    item = MakeString(SerializeToBinaryJson(data));
                } else if (typeIndex == 2) {
                    std::string sample = "7856341212905634789012345678901";
                    std::string data = TStringBuilder() << HexDecode(sample + static_cast<char>('0' + (value % 10)));
                    item = MakeString(NUdf::TStringRef(data.data(), data.size()));
                } else if (typeIndex == 3) {
                    item = NUdf::TUnboxedValuePod(static_cast<float>(value) / 4);
                }

                item = Vb.NewVariant(typeIndex, std::move(item)).MakeOptional();
            } else {
                item = NUdf::TUnboxedValuePod();
            }

            if (value % 3 != 2) {
                item = item.MakeOptional();
            }

            values.push_back(std::move(item));
        }
        return values;
    }

    TType* GetVariantOverTupleWithOptionalsType() {
        TType* members[5] = {
            TDataType::Create(NUdf::TDataType<bool>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<i16>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<ui16>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
            TOptionalType::Create(TDataType::Create(NUdf::TDataType<ui32>::Id, TypeEnv), TypeEnv)
        };
        auto tupleType = TTupleType::Create(5, members, TypeEnv);
        return TVariantType::Create(tupleType, TypeEnv);
    }

    TUnboxedValueVector CreateVariantOverTupleWithOptionals(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto typeIndex = value % 5;
            NUdf::TUnboxedValue item;
            if (typeIndex == 0) {
                item = NUdf::TUnboxedValuePod(value % 3 == 0);
            } else if (typeIndex == 1) {
                item = NUdf::TUnboxedValuePod(static_cast<i16>(-value));
            } else if (typeIndex == 2) {
                item = NUdf::TUnboxedValuePod(static_cast<ui16>(value));
            } else if (typeIndex == 3) {
                item = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
            } else if (typeIndex == 4) {
                NUdf::TUnboxedValue innerItem;
                innerItem = value % 2 == 0
                            ? NUdf::TUnboxedValuePod(static_cast<i32>(value))
                            : NUdf::TUnboxedValuePod();
                item = innerItem.MakeOptional();
            }
            auto wrapped = Vb.NewVariant(typeIndex, std::move(item));
            values.emplace_back(std::move(wrapped));
        }
        return values;
    }

    TType* GetOptionalVariantOverTupleWithOptionalsType() {
        return TOptionalType::Create(GetVariantOverTupleWithOptionalsType(), TypeEnv);
    }

    TUnboxedValueVector CreateOptionalVariantOverTupleWithOptionals(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {

            if (value % 2 == 0) {
                values.push_back(NUdf::TUnboxedValuePod());
                continue;
            }

            auto typeIndex = value % 5;
            NUdf::TUnboxedValue item;
            if (typeIndex == 0) {
                item = NUdf::TUnboxedValuePod(value % 3 == 0);
            } else if (typeIndex == 1) {
                item = NUdf::TUnboxedValuePod(static_cast<i16>(-value));
            } else if (typeIndex == 2) {
                item = NUdf::TUnboxedValuePod(static_cast<ui16>(value));
            } else if (typeIndex == 3) {
                item = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
            } else if (typeIndex == 4) {
                NUdf::TUnboxedValue innerItem;
                innerItem = value % 2 == 0
                            ? NUdf::TUnboxedValuePod(static_cast<i32>(value))
                            : NUdf::TUnboxedValuePod();
                item = innerItem.MakeOptional();
            }
            auto wrapped = Vb.NewVariant(typeIndex, std::move(item)).MakeOptional();
            values.emplace_back(std::move(wrapped));
        }
        return values;
    }

    TType* GetDoubleOptionalVariantOverTupleWithOptionalsType() {
        return TOptionalType::Create(GetOptionalVariantOverTupleWithOptionalsType(), TypeEnv);
    }

    TUnboxedValueVector CreateDoubleOptionalVariantOverTupleWithOptionals(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto typeIndex = value % 5;
            NUdf::TUnboxedValue item;

            if (value % 3 == 0) {
                if (typeIndex == 0) {
                    item = NUdf::TUnboxedValuePod(value % 3 == 0);
                } else if (typeIndex == 1) {
                    item = NUdf::TUnboxedValuePod(static_cast<i16>(-value));
                } else if (typeIndex == 2) {
                    item = NUdf::TUnboxedValuePod(static_cast<ui16>(value));
                } else if (typeIndex == 3) {
                    item = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
                } else if (typeIndex == 4) {
                    NUdf::TUnboxedValue innerItem;
                    innerItem = value % 2 == 0
                                ? NUdf::TUnboxedValuePod(static_cast<i32>(value))
                                : NUdf::TUnboxedValuePod();
                    item = innerItem.MakeOptional();
                }

                item = Vb.NewVariant(typeIndex, std::move(item));
            } else {
                item = NUdf::TUnboxedValuePod();
            }

            if (value % 3 != 2) {
                item = item.MakeOptional();
            }

            values.emplace_back(std::move(item));
        }
        return values;
    }

    TType* GetDictOptionalToTupleType() {
        TType* keyType = TOptionalType::Create(TDataType::Create(NUdf::TDataType<double>::Id, TypeEnv), TypeEnv);
        TType* members[2] = {
            TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
            TDataType::Create(NUdf::TDataType<ui32>::Id, TypeEnv),
        };
        TType* payloadType = TTupleType::Create(2, members, TypeEnv);
        return TDictType::Create(keyType, payloadType, TypeEnv);
    }

    TUnboxedValueVector CreateDictOptionalToTuple(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            auto dictBuilder = Vb.NewDict(GetDictOptionalToTupleType(), 0);
            for (ui64 i = 0; i < value * value; ++i) {
                NUdf::TUnboxedValue key;
                if (i == 0) {
                    key = NUdf::TUnboxedValuePod();
                } else {
                    key = NUdf::TUnboxedValuePod(value / 4).MakeOptional();
                }
                NUdf::TUnboxedValue* items;
                auto payload = Vb.NewArray(2, items);
                items[0] = NUdf::TUnboxedValuePod(static_cast<i32>(-value));
                items[1] = NUdf::TUnboxedValuePod(static_cast<ui32>(value));
                dictBuilder->Add(std::move(key), std::move(payload));
            }
            auto dictValue = dictBuilder->Build();
            values.emplace_back(std::move(dictValue));
        }
        return values;
    }

    TType* GetOptionalOfOptionalType() {
        return TOptionalType::Create(
                   TOptionalType::Create(
                       TDataType::Create(NUdf::TDataType<i32>::Id, TypeEnv),
                       TypeEnv),
                   TypeEnv);
    }

    TUnboxedValueVector CreateOptionalOfOptional(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 value = 0; value < quantity; ++value) {
            NUdf::TUnboxedValue element = value % 3 == 0
                                        ? NUdf::TUnboxedValuePod(value).MakeOptional()
                                        : NUdf::TUnboxedValuePod();
            if (value % 3 != 2) {
                element = element.MakeOptional();
            }
            values.emplace_back(std::move(element));
        }
        return values;
    }

    TType* GetLargeVariantType(const ui16 variantSize) {
        VariantSize = variantSize;
        TVector<TType*> tupleTypes;
        tupleTypes.reserve(variantSize);
        for (ui64 index = 0; index < variantSize; ++index) {
            tupleTypes.push_back(TTupleType::Create(BasicTypes.size(), BasicTypes.data(), TypeEnv));
        }
        auto tupleOfTuplesType = TTupleType::Create(variantSize, tupleTypes.data(), TypeEnv);
        return TVariantType::Create(tupleOfTuplesType, TypeEnv);
    }

    TUnboxedValueVector CreateLargeVariant(ui32 quantity) {
        TUnboxedValueVector values;
        for (ui64 index = 0; index < quantity; ++index) {
            NUdf::TUnboxedValue item;
            auto typeIndex = index % VariantSize;
            TUnboxedValueVector tupleItems;
            for (ui64 i = 0; i < BasicTypes.size(); ++i) {
                tupleItems.push_back(GetValueOfBasicType(BasicTypes[i], i + typeIndex));
            }
            auto wrapped = Vb.NewVariant(typeIndex, HolderFactory.VectorAsArray(tupleItems));
            values.emplace_back(std::move(wrapped));
        }
        return values;
    }
};

void AssertUnboxedValuesAreEqual(NUdf::TUnboxedValue& left, NUdf::TUnboxedValue& right, TType* type) {
    switch (type->GetKind()) {
        case TType::EKind::Void:
        case TType::EKind::Null:
        case TType::EKind::EmptyList:
        case TType::EKind::EmptyDict: {
            UNIT_ASSERT(!left.HasValue());
            UNIT_ASSERT(!right.HasValue());
            break;
        }

        case TType::EKind::Data: {
            auto dataType = static_cast<const TDataType*>(type);
            auto dataSlot = *dataType->GetDataSlot().Get();

            switch (dataSlot) {
                case NUdf::EDataSlot::JsonDocument:
                    left = MakeString(NKikimr::NBinaryJson::SerializeToJson(left.AsStringRef()));
                    right = MakeString(NKikimr::NBinaryJson::SerializeToJson(right.AsStringRef()));
                case NUdf::EDataSlot::Json:
                case NUdf::EDataSlot::Yson: {
                    UNIT_ASSERT_VALUES_EQUAL(std::string(left.AsStringRef()), std::string(right.AsStringRef()));
                    break;
                }

                default: {
                    UNIT_ASSERT(NUdf::EquateValues(dataSlot, left, right));
                }
            }
            break;
        }

        case TType::EKind::Optional: {
            UNIT_ASSERT_VALUES_EQUAL(left.HasValue(), right.HasValue());
            if (left.HasValue()) {
                auto innerType = static_cast<const TOptionalType*>(type)->GetItemType();
                NUdf::TUnboxedValue leftInner = left.GetOptionalValue();
                NUdf::TUnboxedValue rightInner = right.GetOptionalValue();
                AssertUnboxedValuesAreEqual(leftInner, rightInner, innerType);
            }
            break;
        }

        case TType::EKind::List: {
            auto listType = static_cast<const TListType*>(type);
            auto itemType = listType->GetItemType();

            auto leftPtr = left.GetElements();
            auto rightPtr = right.GetElements();
            UNIT_ASSERT_VALUES_EQUAL(leftPtr != nullptr, rightPtr != nullptr);

            if (leftPtr != nullptr) {
                auto leftLen = left.GetListLength();
                auto rightLen = right.GetListLength();
                UNIT_ASSERT_VALUES_EQUAL(leftLen, rightLen);

                while (leftLen > 0) {
                    NUdf::TUnboxedValue leftItem = *leftPtr++;
                    NUdf::TUnboxedValue rightItem = *rightPtr++;
                    AssertUnboxedValuesAreEqual(leftItem, rightItem, itemType);
                    --leftLen;
                }
            } else {
                const auto leftIter = left.GetListIterator();
                const auto rightIter = right.GetListIterator();

                NUdf::TUnboxedValue leftItem;
                NUdf::TUnboxedValue rightItem;
                bool leftHasValue = leftIter.Next(leftItem);
                bool rightHasValue = rightIter.Next(leftItem);

                while (leftHasValue && rightHasValue) {
                    AssertUnboxedValuesAreEqual(leftItem, rightItem, itemType);
                    leftHasValue = leftIter.Next(leftItem);
                    rightHasValue = rightIter.Next(leftItem);
                }
                UNIT_ASSERT_VALUES_EQUAL(leftHasValue, rightHasValue);
            }
            break;
        }

        case TType::EKind::Struct: {
            auto structType = static_cast<const TStructType*>(type);
            UNIT_ASSERT_EQUAL(left.GetListLength(), structType->GetMembersCount());
            UNIT_ASSERT_EQUAL(right.GetListLength(), structType->GetMembersCount());
            for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
                auto memberType = structType->GetMemberType(index);
                NUdf::TUnboxedValue leftMember = left.GetElement(index);
                NUdf::TUnboxedValue rightMember = right.GetElement(index);
                AssertUnboxedValuesAreEqual(leftMember, rightMember, memberType);
            }
            break;
        }

        case TType::EKind::Tuple: {
            auto tupleType = static_cast<const TTupleType*>(type);

            UNIT_ASSERT_EQUAL(left.GetListLength(), tupleType->GetElementsCount());
            UNIT_ASSERT_EQUAL(right.GetListLength(), tupleType->GetElementsCount());

            for (ui32 index = 0; index < tupleType->GetElementsCount(); ++index) {
                auto elementType = tupleType->GetElementType(index);
                NUdf::TUnboxedValue leftMember = left.GetElement(index);
                NUdf::TUnboxedValue rightMember = right.GetElement(index);
                AssertUnboxedValuesAreEqual(leftMember, rightMember, elementType);
            }
            break;
        }

        // case TType::EKind::Dict: {
        //     auto dictType = static_cast<const TDictType*>(type);
        //     auto payloadType = dictType->GetPayloadType();

        //     UNIT_ASSERT_EQUAL(left.GetDictLength(), right.GetDictLength());
        //     const auto leftIter = left.GetDictIterator();
        //     for (NUdf::TUnboxedValue key, leftPayload; leftIter.NextPair(key, leftPayload);) {
        //         UNIT_ASSERT(right.Contains(key));
        //         NUdf::TUnboxedValue rightPayload = right.Lookup(key);
        //         AssertUnboxedValuesAreEqual(leftPayload, rightPayload, payloadType);
        //     }
        //     break;
        // }

        // case TType::EKind::Variant: {
        //     auto variantType = static_cast<const TVariantType*>(type);
        //     UNIT_ASSERT_EQUAL(left.GetVariantIndex(), right.GetVariantIndex());
        //     ui32 variantIndex = left.GetVariantIndex();
        //     TType* innerType = variantType->GetUnderlyingType();
        //     if (innerType->IsStruct()) {
        //         innerType = static_cast<TStructType*>(innerType)->GetMemberType(variantIndex);
        //     } else {
        //         UNIT_ASSERT_C(innerType->IsTuple(), "Unexpected underlying variant type: " << innerType->GetKindAsStr());
        //         innerType = static_cast<TTupleType*>(innerType)->GetElementType(variantIndex);
        //     }
        //     NUdf::TUnboxedValue leftValue = left.GetVariantItem();
        //     NUdf::TUnboxedValue rightValue = right.GetVariantItem();
        //     AssertUnboxedValuesAreEqual(leftValue, rightValue, innerType);
        //     break;
        // }

        default: {
            UNIT_ASSERT_C(false, TStringBuilder() << "Unsupported type: " << type->GetKindAsStr());
        }
    }
}

} // namespace

namespace NKikimr::NKqp::NFormats {

namespace {

template <typename TMiniKQLType, typename TPhysicalType, typename TArrowArrayType, bool IsStringType = false, bool IsTimezoneType = false>
void TestDataTypeConversion(arrow::Type::type arrowTypeId) {
    TTestContext context;

    auto type = TDataType::Create(NUdf::TDataType<TMiniKQLType>::Id, context.TypeEnv);
    UNIT_ASSERT(IsArrowCompatible(type));

    TUnboxedValueVector values;
    values.reserve(TEST_ARRAY_DATATYPE_SIZE);

    for (size_t i = 0; i < TEST_ARRAY_DATATYPE_SIZE; ++i) {
        values.emplace_back(GetValueOfBasicType(type, i));
    }

    auto array = MakeArrowArray(values, type);
    UNIT_ASSERT_C(array->ValidateFull().ok(), array->ValidateFull().ToString());
    UNIT_ASSERT_VALUES_EQUAL(array->length(), values.size());

    std::shared_ptr<TArrowArrayType> typedArray;
    std::shared_ptr<arrow::StringArray> timezoneArray;

    if constexpr (IsTimezoneType) {
        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT_VALUES_EQUAL(structArray->num_fields(), 2);

        UNIT_ASSERT(structArray->field(0)->type_id() == arrowTypeId);
        UNIT_ASSERT(structArray->field(1)->type_id() == arrow::Type::STRING);

        typedArray = static_pointer_cast<TArrowArrayType>(structArray->field(0));
        timezoneArray = static_pointer_cast<arrow::StringArray>(structArray->field(1));
    } else {
        UNIT_ASSERT(array->type_id() == arrowTypeId);
        typedArray = static_pointer_cast<TArrowArrayType>(array);
    }

    for (size_t i = 0; i < TEST_ARRAY_DATATYPE_SIZE; ++i) {
        auto arrowValue = ExtractUnboxedValue(array, i, type, context.HolderFactory);
        AssertUnboxedValuesAreEqual(arrowValue, values[i], type);
    }
}

template <typename TMiniKQLType, bool IsDecimalType = false>
void TestFixedSizeBinaryDataTypeConversion() {
    TTestContext context;
    TType* type;

    if constexpr (IsDecimalType) {
        type = TDataDecimalType::Create(35, 10, context.TypeEnv);
    } else {
        type = TDataType::Create(NUdf::TDataType<TMiniKQLType>::Id, context.TypeEnv);
    }

    UNIT_ASSERT(IsArrowCompatible(type));

    TUnboxedValueVector values;
    values.reserve(TEST_ARRAY_DATATYPE_SIZE);

    for (size_t i = 0; i < TEST_ARRAY_DATATYPE_SIZE; ++i) {
        values.emplace_back(GetValueOfBasicType(type, i));
    }

    auto array = MakeArrowArray(values, type);
    UNIT_ASSERT_C(array->ValidateFull().ok(), array->ValidateFull().ToString());
    UNIT_ASSERT_VALUES_EQUAL(array->length(), values.size());

    std::shared_ptr<arrow::FixedSizeBinaryArray> typedArray;

    UNIT_ASSERT(array->type_id() == arrow::Type::FIXED_SIZE_BINARY);
    typedArray = static_pointer_cast<arrow::FixedSizeBinaryArray>(array);
    UNIT_ASSERT_VALUES_EQUAL(typedArray->byte_width(), NScheme::FSB_SIZE);

    for (size_t i = 0; i < TEST_ARRAY_DATATYPE_SIZE; ++i) {
        auto arrowValue = ExtractUnboxedValue(array, i, type, context.HolderFactory);
        AssertUnboxedValuesAreEqual(arrowValue, values[i], type);
    }
}

template <TType::EKind SingularKind>
void TestSingularTypeConversion() {
    TTestContext context;

    TType* type = GetTypeOfSingular<SingularKind>(context.TypeEnv);
    UNIT_ASSERT(IsArrowCompatible(type));

    TUnboxedValueVector values;
    values.reserve(TEST_ARRAY_DATATYPE_SIZE);

    for (size_t i = 0; i < TEST_ARRAY_DATATYPE_SIZE; ++i) {
        values.emplace_back();
    }

    auto array = MakeArrowArray(values, type);
    UNIT_ASSERT_C(array->ValidateFull().ok(), array->ValidateFull().ToString());
    UNIT_ASSERT_VALUES_EQUAL(array->length(), TEST_ARRAY_DATATYPE_SIZE);

    if (SingularKind == TType::EKind::Null) {
        UNIT_ASSERT(array->type_id() == arrow::Type::NA);
    } else {
        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT_VALUES_EQUAL(structArray->num_fields(), 0);
    }

    for (size_t i = 0; i < TEST_ARRAY_DATATYPE_SIZE; ++i) {
        auto arrowValue = ExtractUnboxedValue(array, i, type, context.HolderFactory);
        AssertUnboxedValuesAreEqual(arrowValue, values[i], type);
    }
}

} // namespace

Y_UNIT_TEST_SUITE(KqpFormats_Arrow_Conversion) {

    // Integral types
    Y_UNIT_TEST(DataType_Bool) {
        TestDataTypeConversion<bool, bool, arrow::UInt8Array>(arrow::Type::UINT8);
    }

    Y_UNIT_TEST(DataType_Int8) {
        TestDataTypeConversion<i8, i8, arrow::Int8Array>(arrow::Type::INT8);
    }

    Y_UNIT_TEST(DataType_UInt8) {
        TestDataTypeConversion<ui8, ui8, arrow::UInt8Array>(arrow::Type::UINT8);
    }

    Y_UNIT_TEST(DataType_Int16) {
        TestDataTypeConversion<i16, i16, arrow::Int16Array>(arrow::Type::INT16);
    }

    Y_UNIT_TEST(DataType_UInt16) {
        TestDataTypeConversion<ui16, ui16, arrow::UInt16Array>(arrow::Type::UINT16);
    }

    Y_UNIT_TEST(DataType_Int32) {
        TestDataTypeConversion<i32, i32, arrow::Int32Array>(arrow::Type::INT32);
    }

    Y_UNIT_TEST(DataType_UInt32) {
        TestDataTypeConversion<ui32, ui32, arrow::UInt32Array>(arrow::Type::UINT32);
    }

    Y_UNIT_TEST(DataType_Int64) {
        TestDataTypeConversion<i64, i64, arrow::Int64Array>(arrow::Type::INT64);
    }

    Y_UNIT_TEST(DataType_UInt64) {
        TestDataTypeConversion<ui64, ui64, arrow::UInt64Array>(arrow::Type::UINT64);
    }

    // Binary number types
    Y_UNIT_TEST(DataType_Decimal) {
        TestFixedSizeBinaryDataTypeConversion<NUdf::TDecimal, /* IsDecimalType */ true>();
    }

    Y_UNIT_TEST(DataType_DyNumber) {
        TestDataTypeConversion<NUdf::TDyNumber, std::string, arrow::StringArray, /* IsStringType */ true>(arrow::Type::STRING);
    }

    // Floating point types
    Y_UNIT_TEST(DataType_Float) {
        TestDataTypeConversion<float, float, arrow::FloatArray>(arrow::Type::FLOAT);
    }

    Y_UNIT_TEST(DataType_Double) {
        TestDataTypeConversion<double, double, arrow::DoubleArray>(arrow::Type::DOUBLE);
    }

    // Datetime types
    Y_UNIT_TEST(DataType_Date) {
        TestDataTypeConversion<NUdf::TDate, ui16, arrow::UInt16Array>(arrow::Type::UINT16);
    }

    Y_UNIT_TEST(DataType_Datetime) {
        TestDataTypeConversion<NUdf::TDatetime, ui32, arrow::UInt32Array>(arrow::Type::UINT32);
    }

    Y_UNIT_TEST(DataType_Timestamp) {
        TestDataTypeConversion<NUdf::TTimestamp, ui64, arrow::UInt64Array>(arrow::Type::UINT64);
    }

    Y_UNIT_TEST(DataType_Interval) {
        TestDataTypeConversion<NUdf::TInterval, i64, arrow::Int64Array>(arrow::Type::INT64);
    }

    Y_UNIT_TEST(DataType_TzDate) {
        TestDataTypeConversion<NUdf::TTzDate, ui16, arrow::UInt16Array, /* IsStringType */ false, /* HasTimezone */ true>(arrow::Type::UINT16);
    }

    Y_UNIT_TEST(DataType_TzDatetime) {
        TestDataTypeConversion<NUdf::TTzDatetime, ui32, arrow::UInt32Array, /* IsStringType */ false, /* HasTimezone */ true>(arrow::Type::UINT32);
    }

    Y_UNIT_TEST(DataType_TzTimestamp) {
        TestDataTypeConversion<NUdf::TTzTimestamp, ui64, arrow::UInt64Array, /* IsStringType */ false, /* HasTimezone */ true>(arrow::Type::UINT64);
    }

    Y_UNIT_TEST(DataType_Date32) {
        TestDataTypeConversion<NUdf::TDate32, i32, arrow::Int32Array>(arrow::Type::INT32);
    }

    Y_UNIT_TEST(DataType_Datetime64) {
        TestDataTypeConversion<NUdf::TDatetime64, i64, arrow::Int64Array>(arrow::Type::INT64);
    }

    Y_UNIT_TEST(DataType_Timestamp64) {
        TestDataTypeConversion<NUdf::TTimestamp64, i64, arrow::Int64Array>(arrow::Type::INT64);
    }

    Y_UNIT_TEST(DataType_Interval64) {
        TestDataTypeConversion<NUdf::TInterval64, i64, arrow::Int64Array>(arrow::Type::INT64);
    }

    Y_UNIT_TEST(DataType_TzDate32) {
        TestDataTypeConversion<NUdf::TTzDate32, i32, arrow::Int32Array, /* IsStringType */ false, /* HasTimezone */ true>(arrow::Type::INT32);
    }

    Y_UNIT_TEST(DataType_TzDatetime64) {
        TestDataTypeConversion<NUdf::TTzDatetime64, i64, arrow::Int64Array, /* IsStringType */ false, /* HasTimezone */ true>(arrow::Type::INT64);
    }

    Y_UNIT_TEST(DataType_TzTimestamp64) {
        TestDataTypeConversion<NUdf::TTzTimestamp64, i64, arrow::Int64Array, /* IsStringType */ false, /* HasTimezone */ true>(arrow::Type::INT64);
    }

    // String types
    Y_UNIT_TEST(DataType_String) {
        TestDataTypeConversion<char*, std::string, arrow::BinaryArray, /* IsStringType */ true>(arrow::Type::BINARY);
    }

    Y_UNIT_TEST(DataType_Utf8) {
        TestDataTypeConversion<NUdf::TUtf8, std::string, arrow::StringArray, /* IsStringType */ true>(arrow::Type::STRING);
    }

    Y_UNIT_TEST(DataType_Yson) {
        TestDataTypeConversion<NUdf::TYson, std::string, arrow::BinaryArray, /* IsStringType */ true>(arrow::Type::BINARY);
    }

    Y_UNIT_TEST(DataType_Json) {
        TestDataTypeConversion<NUdf::TJson, std::string, arrow::StringArray, /* IsStringType */ true>(arrow::Type::STRING);
    }

    Y_UNIT_TEST(DataType_JsonDocument) {
        TestDataTypeConversion<NUdf::TJsonDocument, std::string, arrow::StringArray, /* IsStringType */ true>(arrow::Type::STRING);
    }

    Y_UNIT_TEST(DataType_Uuid) {
        TestFixedSizeBinaryDataTypeConversion<NUdf::TUuid>();
    }

    // Singular types
    Y_UNIT_TEST(DataType_Null) {
        TestSingularTypeConversion<TType::EKind::Null>();
    }

    Y_UNIT_TEST(DataType_Void) {
        TestSingularTypeConversion<TType::EKind::Void>();
    }

    Y_UNIT_TEST(DataType_EmptyList) {
        TestSingularTypeConversion<TType::EKind::EmptyList>();
    }

    Y_UNIT_TEST(DataType_EmptyDict) {
        TestSingularTypeConversion<TType::EKind::EmptyDict>();
    }

    // Nested types
    Y_UNIT_TEST(NestedType_List) {
        TTestContext context;

        auto listType = context.GetListType();
        auto values = context.CreateLists(TEST_ARRAY_NESTED_SIZE);

        UNIT_ASSERT(IsArrowCompatible(listType));

        auto array = MakeArrowArray(values, listType);
        UNIT_ASSERT_C(array->ValidateFull().ok(), array->ValidateFull().ToString());
        UNIT_ASSERT_VALUES_EQUAL(array->length(), values.size());

        UNIT_ASSERT(array->type_id() == arrow::Type::LIST);
        auto listArray = static_pointer_cast<arrow::ListArray>(array);
        UNIT_ASSERT_VALUES_EQUAL(listArray->num_fields(), 1);
        UNIT_ASSERT(listArray->value_type()->id() == arrow::Type::INT32);

        for (size_t i = 0; i < values.size(); ++i) {
            auto arrowValue = ExtractUnboxedValue(array, i, listType, context.HolderFactory);
            AssertUnboxedValuesAreEqual(arrowValue, values[i], listType);
        }
    }

    Y_UNIT_TEST(NestedType_Tuple) {
        TTestContext context;

        auto tupleType = context.GetTupleType();
        auto values = context.CreateTuples(TEST_ARRAY_NESTED_SIZE);

        UNIT_ASSERT(IsArrowCompatible(tupleType));

        auto array = MakeArrowArray(values, tupleType);
        UNIT_ASSERT_C(array->ValidateFull().ok(), array->ValidateFull().ToString());
        UNIT_ASSERT_VALUES_EQUAL(array->length(), values.size());

        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT_VALUES_EQUAL(structArray->num_fields(), 3);

        UNIT_ASSERT(structArray->field(0)->type_id() == arrow::Type::UINT8);
        UNIT_ASSERT(structArray->field(1)->type_id() == arrow::Type::INT8);
        UNIT_ASSERT(structArray->field(2)->type_id() == arrow::Type::UINT8);

        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(structArray->field(0)->length()), values.size());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(structArray->field(1)->length()), values.size());
        UNIT_ASSERT_VALUES_EQUAL(static_cast<ui64>(structArray->field(2)->length()), values.size());

        for (size_t i = 0; i < values.size(); ++i) {
            auto arrowValue = ExtractUnboxedValue(array, i, tupleType, context.HolderFactory);
            AssertUnboxedValuesAreEqual(arrowValue, values[i], tupleType);
        }
    }

    Y_UNIT_TEST(NestedType_Struct) {
        TTestContext context;

        auto structType = context.GetStructType();
        auto values = context.CreateStructs(TEST_ARRAY_NESTED_SIZE);

        UNIT_ASSERT(IsArrowCompatible(structType));

        auto array = MakeArrowArray(values, structType);
        UNIT_ASSERT_C(array->ValidateFull().ok(), array->ValidateFull().ToString());
        UNIT_ASSERT_VALUES_EQUAL(array->length(), values.size());

        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT_VALUES_EQUAL(structArray->num_fields(), 5);

        UNIT_ASSERT(structArray->GetFieldByName("ABC") && structArray->GetFieldByName("ABC")->type_id() == arrow::Type::BINARY);
        UNIT_ASSERT(structArray->GetFieldByName("DEF") && structArray->GetFieldByName("DEF")->type_id() == arrow::Type::INT32);
        UNIT_ASSERT(structArray->GetFieldByName("GHI") && structArray->GetFieldByName("GHI")->type_id() == arrow::Type::UINT64);
        UNIT_ASSERT(structArray->GetFieldByName("JKL") && structArray->GetFieldByName("JKL")->type_id() == arrow::Type::INT64);
        UNIT_ASSERT(structArray->GetFieldByName("MNO") && structArray->GetFieldByName("MNO")->type_id() == arrow::Type::STRING);

        UNIT_ASSERT_VALUES_EQUAL(structArray->GetFieldByName("ABC")->length(), values.size());
        UNIT_ASSERT_VALUES_EQUAL(structArray->GetFieldByName("DEF")->length(), values.size());
        UNIT_ASSERT_VALUES_EQUAL(structArray->GetFieldByName("GHI")->length(), values.size());
        UNIT_ASSERT_VALUES_EQUAL(structArray->GetFieldByName("JKL")->length(), values.size());
        UNIT_ASSERT_VALUES_EQUAL(structArray->GetFieldByName("MNO")->length(), values.size());

        for (size_t i = 0; i < values.size(); ++i) {
            auto arrowValue = ExtractUnboxedValue(array, i, structType, context.HolderFactory);
            AssertUnboxedValuesAreEqual(arrowValue, values[i], structType);
        }
    }
}

Y_UNIT_TEST_SUITE(DqUnboxedValueToNativeArrowConversion) {
    Y_UNIT_TEST(OptionalListOfOptional) {
        TTestContext context;

        auto listType = context.GetOptionalListOfOptional();
        Y_ABORT_UNLESS(IsArrowCompatible(listType));

        auto values = context.CreateOptionalListOfOptional(100);
        auto array = MakeArrowArray(values, listType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::LIST);

        auto listArray = static_pointer_cast<arrow::ListArray>(array);
        UNIT_ASSERT(listArray->num_fields() == 1);
        UNIT_ASSERT(listArray->value_type()->id() == arrow::Type::INT32);

        auto i32Array = static_pointer_cast<arrow::Int32Array>(listArray->values());
        auto index = 0;
        auto innerIndex = 0;
        for (const auto& value: values) {
            if (!value.HasValue()) {
                UNIT_ASSERT(listArray->IsNull(index));
                ++index;
                continue;
            }

            auto listValue = value.GetOptionalValue();

            UNIT_ASSERT_VALUES_EQUAL(listValue.GetListLength(), static_cast<ui64>(listArray->value_length(index)));
            const auto iter = listValue.GetListIterator();
            for (NUdf::TUnboxedValue item; iter.Next(item);) {
                if (!item.HasValue()) {
                    UNIT_ASSERT(i32Array->IsNull(innerIndex));
                } else {
                    UNIT_ASSERT(i32Array->Value(innerIndex) == item.GetOptionalValue().Get<i32>());
                }
                ++innerIndex;
            }
            ++index;
        }
    }

    // Y_UNIT_TEST(VariantOverStruct) {
    //     TTestContext context;

    //     auto variantType = context.GetVariantOverStructType();
    //     UNIT_ASSERT(IsArrowCompatible(variantType));

    //     auto values = context.CreateVariantOverStruct(100);
    //     auto array = MakeArrowArray(values, variantType);
    //     UNIT_ASSERT(array->ValidateFull().ok());
    //     UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
    //     UNIT_ASSERT(array->type_id() == arrow::Type::DENSE_UNION);
    //     auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);

    //     UNIT_ASSERT(unionArray->num_fields() == 4);
    //     UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::BINARY);
    //     UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::STRING);
    //     UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::FIXED_SIZE_BINARY);
    //     UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::FLOAT);

    //     auto ysonArray = static_pointer_cast<arrow::BinaryArray>(unionArray->field(0));
    //     auto jsonDocArray = static_pointer_cast<arrow::BinaryArray>(unionArray->field(1));
    //     auto uuidArray = static_pointer_cast<arrow::FixedSizeBinaryArray>(unionArray->field(2));
    //     auto floatArray = static_pointer_cast<arrow::FloatArray>(unionArray->field(3));

    //     for (ui64 index = 0; index < values.size(); ++index) {
    //         auto value = values[index];
    //         UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
    //         auto fieldIndex = unionArray->value_offset(index);
    //         if (value.GetVariantIndex() == 3) {
    //             auto valueArrow = floatArray->Value(fieldIndex);
    //             auto valueInner = value.GetVariantItem().Get<float>();
    //             UNIT_ASSERT(valueArrow == valueInner);
    //         } else {
    //             arrow::util::string_view viewArrow;
    //             if (value.GetVariantIndex() == 0) {
    //                 viewArrow = ysonArray->GetView(fieldIndex);
    //             } else if (value.GetVariantIndex() == 1) {
    //                 viewArrow = jsonDocArray->GetView(fieldIndex);
    //             } else if (value.GetVariantIndex() == 2) {
    //                 viewArrow = uuidArray->GetView(fieldIndex);
    //             }
    //             std::string valueArrow(viewArrow.data(), viewArrow.size());
    //             auto innerItem = value.GetVariantItem();
    //             auto refInner = innerItem.AsStringRef();
    //             std::string valueInner(refInner.Data(), refInner.Size());
    //             UNIT_ASSERT(valueArrow == valueInner);
    //         }
    //     }
    // }

    // Y_UNIT_TEST(OptionalVariantOverStruct) {
    //     TTestContext context;

    //     auto variantType = context.GetOptionalVariantOverStructType();
    //     UNIT_ASSERT(IsArrowCompatible(variantType));

    //     auto values = context.CreateOptionalVariantOverStruct(100);
    //     auto array = MakeArrowArray(values, variantType);
    //     UNIT_ASSERT(array->ValidateFull().ok());
    //     UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
    //     UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);

    //     auto structArray = static_pointer_cast<arrow::StructArray>(array);
    //     UNIT_ASSERT(structArray->num_fields() == 1);
    //     UNIT_ASSERT(structArray->field(0)->type_id() == arrow::Type::DENSE_UNION);

    //     auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(structArray->field(0));

    //     UNIT_ASSERT(unionArray->num_fields() == 4);
    //     UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::BINARY);
    //     UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::STRING);
    //     UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::FIXED_SIZE_BINARY);
    //     UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::FLOAT);

    //     auto ysonArray = static_pointer_cast<arrow::BinaryArray>(unionArray->field(0));
    //     auto jsonDocArray = static_pointer_cast<arrow::BinaryArray>(unionArray->field(1));
    //     auto uuidArray = static_pointer_cast<arrow::FixedSizeBinaryArray>(unionArray->field(2));
    //     auto floatArray = static_pointer_cast<arrow::FloatArray>(unionArray->field(3));

    //     for (ui64 index = 0; index < values.size(); ++index) {
    //         auto value = values[index];
    //         if (!value.HasValue()) {
    //             // NULL
    //             UNIT_ASSERT(structArray->IsNull(index));
    //             continue;
    //         }

    //         UNIT_ASSERT(!structArray->IsNull(index));

    //         UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
    //         auto fieldIndex = unionArray->value_offset(index);
    //         if (value.GetVariantIndex() == 3) {
    //             auto valueArrow = floatArray->Value(fieldIndex);
    //             auto valueInner = value.GetVariantItem().Get<float>();
    //             UNIT_ASSERT(valueArrow == valueInner);
    //         } else {
    //             arrow::util::string_view viewArrow;
    //             if (value.GetVariantIndex() == 0) {
    //                 viewArrow = ysonArray->GetView(fieldIndex);
    //             } else if (value.GetVariantIndex() == 1) {
    //                 viewArrow = jsonDocArray->GetView(fieldIndex);
    //             } else if (value.GetVariantIndex() == 2) {
    //                 viewArrow = uuidArray->GetView(fieldIndex);
    //             }
    //             std::string valueArrow(viewArrow.data(), viewArrow.size());
    //             auto innerItem = value.GetVariantItem();
    //             auto refInner = innerItem.AsStringRef();
    //             std::string valueInner(refInner.Data(), refInner.Size());
    //             UNIT_ASSERT(valueArrow == valueInner);
    //         }
    //     }
    // }

    // Y_UNIT_TEST(DoubleOptionalVariantOverStruct) {
    //     TTestContext context;

    //     auto variantType = context.GetDoubleOptionalVariantOverStructType();
    //     UNIT_ASSERT(IsArrowCompatible(variantType));

    //     auto values = context.CreateDoubleOptionalVariantOverStruct(100);
    //     auto array = MakeArrowArray(values, variantType);
    //     UNIT_ASSERT(array->ValidateFull().ok());
    //     UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
    //     UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);

    //     auto firstStructArray = static_pointer_cast<arrow::StructArray>(array);
    //     UNIT_ASSERT(firstStructArray->num_fields() == 1);
    //     UNIT_ASSERT(firstStructArray->field(0)->type_id() == arrow::Type::STRUCT);

    //     auto secondStructArray = static_pointer_cast<arrow::StructArray>(firstStructArray->field(0));
    //     UNIT_ASSERT(secondStructArray->num_fields() == 1);
    //     UNIT_ASSERT(secondStructArray->field(0)->type_id() == arrow::Type::DENSE_UNION);

    //     auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(secondStructArray->field(0));

    //     UNIT_ASSERT(unionArray->num_fields() == 4);
    //     UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::BINARY);
    //     UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::STRING);
    //     UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::FIXED_SIZE_BINARY);
    //     UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::FLOAT);

    //     auto ysonArray = static_pointer_cast<arrow::BinaryArray>(unionArray->field(0));
    //     auto jsonDocArray = static_pointer_cast<arrow::BinaryArray>(unionArray->field(1));
    //     auto uuidArray = static_pointer_cast<arrow::FixedSizeBinaryArray>(unionArray->field(2));
    //     auto floatArray = static_pointer_cast<arrow::FloatArray>(unionArray->field(3));

    //     for (ui64 index = 0; index < values.size(); ++index) {
    //         auto value = values[index];
    //         if (!value.HasValue()) {
    //             if (value) {
    //                 // Optional(NULL)
    //                 UNIT_ASSERT(secondStructArray->IsNull(index));
    //             } else {
    //                 // NULL
    //                 UNIT_ASSERT(firstStructArray->IsNull(index));
    //             }
    //             continue;
    //         }

    //         UNIT_ASSERT(!firstStructArray->IsNull(index) && !secondStructArray->IsNull(index));

    //         UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
    //         auto fieldIndex = unionArray->value_offset(index);
    //         if (value.GetVariantIndex() == 3) {
    //             auto valueArrow = floatArray->Value(fieldIndex);
    //             auto valueInner = value.GetVariantItem().Get<float>();
    //             UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
    //         } else {
    //             arrow::util::string_view viewArrow;
    //             if (value.GetVariantIndex() == 0) {
    //                 viewArrow = ysonArray->GetView(fieldIndex);
    //             } else if (value.GetVariantIndex() == 1) {
    //                 viewArrow = jsonDocArray->GetView(fieldIndex);
    //             } else if (value.GetVariantIndex() == 2) {
    //                 viewArrow = uuidArray->GetView(fieldIndex);
    //             }
    //             std::string valueArrow(viewArrow.data(), viewArrow.size());
    //             auto innerItem = value.GetVariantItem();
    //             auto refInner = innerItem.AsStringRef();
    //             std::string valueInner(refInner.Data(), refInner.Size());
    //             UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
    //         }
    //     }
    // }

    Y_UNIT_TEST(VariantOverTupleWithOptionals) {
        TTestContext context;

        auto variantType = context.GetVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(IsArrowCompatible(variantType));

        auto values = context.CreateVariantOverTupleWithOptionals(100);
        auto array = MakeArrowArray(values, variantType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::DENSE_UNION);
        auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);

        UNIT_ASSERT(unionArray->num_fields() == 5);
        UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::UINT8);
        UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::INT16);
        UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::UINT16);
        UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::INT32);
        UNIT_ASSERT(unionArray->field(4)->type_id() == arrow::Type::UINT32);
        auto boolArray = static_pointer_cast<arrow::UInt8Array>(unionArray->field(0));
        auto i16Array = static_pointer_cast<arrow::Int16Array>(unionArray->field(1));
        auto ui16Array = static_pointer_cast<arrow::UInt16Array>(unionArray->field(2));
        auto i32Array = static_pointer_cast<arrow::Int32Array>(unionArray->field(3));
        auto ui32Array = static_pointer_cast<arrow::UInt32Array>(unionArray->field(4));
        for (ui64 index = 0; index < values.size(); ++index) {
            auto value = values[index];
            UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
            auto fieldIndex = unionArray->value_offset(index);
            if (value.GetVariantIndex() == 0) {
                bool valueArrow = boolArray->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<bool>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 1) {
                auto valueArrow = i16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i16>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 2) {
                auto valueArrow = ui16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<ui16>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 3) {
                auto valueArrow = i32Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i32>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 4) {
                if (!value.GetVariantItem().HasValue()) {
                    UNIT_ASSERT(ui32Array->IsNull(fieldIndex));
                } else {
                    auto valueArrow = ui32Array->Value(fieldIndex);
                    auto valueInner = value.GetVariantItem().Get<ui32>();
                    UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
                }
            }
        }
    }

    Y_UNIT_TEST(OptionalVariantOverTupleWithOptionals) {
        // DenseUnionArray does not support NULL values, so we wrap it in a StructArray

        TTestContext context;

        auto variantType = context.GetOptionalVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(IsArrowCompatible(variantType));

        auto values = context.CreateOptionalVariantOverTupleWithOptionals(100);
        auto array = MakeArrowArray(values, variantType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);

        auto structArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT(structArray->num_fields() == 1);
        UNIT_ASSERT(structArray->field(0)->type_id() == arrow::Type::DENSE_UNION);

        auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(structArray->field(0));
        UNIT_ASSERT(unionArray->num_fields() == 5);
        UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::UINT8);
        UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::INT16);
        UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::UINT16);
        UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::INT32);
        UNIT_ASSERT(unionArray->field(4)->type_id() == arrow::Type::UINT32);
        auto boolArray = static_pointer_cast<arrow::UInt8Array>(unionArray->field(0));
        auto i16Array = static_pointer_cast<arrow::Int16Array>(unionArray->field(1));
        auto ui16Array = static_pointer_cast<arrow::UInt16Array>(unionArray->field(2));
        auto i32Array = static_pointer_cast<arrow::Int32Array>(unionArray->field(3));
        auto ui32Array = static_pointer_cast<arrow::UInt32Array>(unionArray->field(4));
        for (ui64 index = 0; index < values.size(); ++index) {
            auto value = values[index];
            if (!value) {
                // NULL
                UNIT_ASSERT(structArray->IsNull(index));
                continue;
            }

            UNIT_ASSERT(!structArray->IsNull(index));

            UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
            auto fieldIndex = unionArray->value_offset(index);
            if (value.GetVariantIndex() == 0) {
                bool valueArrow = boolArray->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<bool>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 1) {
                auto valueArrow = i16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i16>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 2) {
                auto valueArrow = ui16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<ui16>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 3) {
                auto valueArrow = i32Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i32>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 4) {
                if (!value.GetVariantItem().HasValue()) {
                    UNIT_ASSERT(ui32Array->IsNull(fieldIndex));
                } else {
                    auto valueArrow = ui32Array->Value(fieldIndex);
                    auto valueInner = value.GetVariantItem().Get<ui32>();
                    UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
                }
            }
        }
    }

    Y_UNIT_TEST(DoubleOptionalVariantOverTupleWithOptionals) {
        // DenseUnionArray does not support NULL values, so we wrap it in a StructArray

        TTestContext context;

        auto variantType = context.GetDoubleOptionalVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(IsArrowCompatible(variantType));

        auto values = context.CreateDoubleOptionalVariantOverTupleWithOptionals(100);
        auto array = MakeArrowArray(values, variantType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT(static_cast<ui64>(array->length()) == values.size());
        UNIT_ASSERT(array->type_id() == arrow::Type::STRUCT);

        auto firstStructArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT(firstStructArray->num_fields() == 1);
        UNIT_ASSERT(firstStructArray->field(0)->type_id() == arrow::Type::STRUCT);

        auto secondStructArray = static_pointer_cast<arrow::StructArray>(firstStructArray->field(0));
        UNIT_ASSERT(secondStructArray->num_fields() == 1);
        UNIT_ASSERT(secondStructArray->field(0)->type_id() == arrow::Type::DENSE_UNION);

        auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(secondStructArray->field(0));
        UNIT_ASSERT(unionArray->num_fields() == 5);
        UNIT_ASSERT(unionArray->field(0)->type_id() == arrow::Type::UINT8);
        UNIT_ASSERT(unionArray->field(1)->type_id() == arrow::Type::INT16);
        UNIT_ASSERT(unionArray->field(2)->type_id() == arrow::Type::UINT16);
        UNIT_ASSERT(unionArray->field(3)->type_id() == arrow::Type::INT32);
        UNIT_ASSERT(unionArray->field(4)->type_id() == arrow::Type::UINT32);
        auto boolArray = static_pointer_cast<arrow::UInt8Array>(unionArray->field(0));
        auto i16Array = static_pointer_cast<arrow::Int16Array>(unionArray->field(1));
        auto ui16Array = static_pointer_cast<arrow::UInt16Array>(unionArray->field(2));
        auto i32Array = static_pointer_cast<arrow::Int32Array>(unionArray->field(3));
        auto ui32Array = static_pointer_cast<arrow::UInt32Array>(unionArray->field(4));
        for (ui64 index = 0; index < values.size(); ++index) {
            auto value = values[index];
            if (!value.HasValue()) {
                if (value && !value.GetOptionalValue()) {
                    // Optional(NULL)
                    UNIT_ASSERT(secondStructArray->IsNull(index));
                } else if (!value) {
                    // NULL
                    UNIT_ASSERT(firstStructArray->IsNull(index));
                }
                continue;
            }

            UNIT_ASSERT(!firstStructArray->IsNull(index) && !secondStructArray->IsNull(index));

            UNIT_ASSERT(value.GetVariantIndex() == static_cast<ui32>(unionArray->child_id(index)));
            auto fieldIndex = unionArray->value_offset(index);
            if (value.GetVariantIndex() == 0) {
                bool valueArrow = boolArray->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<bool>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 1) {
                auto valueArrow = i16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i16>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 2) {
                auto valueArrow = ui16Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<ui16>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 3) {
                auto valueArrow = i32Array->Value(fieldIndex);
                auto valueInner = value.GetVariantItem().Get<i32>();
                UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
            } else if (value.GetVariantIndex() == 4) {
                if (!value.GetVariantItem().HasValue()) {
                    UNIT_ASSERT(ui32Array->IsNull(fieldIndex));
                } else {
                    auto valueArrow = ui32Array->Value(fieldIndex);
                    auto valueInner = value.GetVariantItem().Get<ui32>();
                    UNIT_ASSERT_VALUES_EQUAL(valueArrow, valueInner);
                }
            }
        }
    }
}

Y_UNIT_TEST_SUITE(DqUnboxedValueDoNotFitToArrow) {
    Y_UNIT_TEST(DictOptionalToTuple) {
        TTestContext context;

        auto dictType = context.GetDictOptionalToTupleType();
        UNIT_ASSERT(IsArrowCompatible(dictType));

        auto values = context.CreateDictOptionalToTuple(100);
        auto array = MakeArrowArray(values, dictType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT_EQUAL(static_cast<ui64>(array->length()), values.size());
        UNIT_ASSERT_EQUAL(array->type_id(), arrow::Type::STRUCT);

        auto wrapArray = static_pointer_cast<arrow::StructArray>(array);
        UNIT_ASSERT_EQUAL(wrapArray->num_fields(), 2);
        UNIT_ASSERT_EQUAL(wrapArray->field(0)->type_id(), arrow::Type::LIST);

        UNIT_ASSERT_EQUAL(wrapArray->field(1)->type_id(), arrow::Type::UINT64);
        auto listArray = static_pointer_cast<arrow::ListArray>(wrapArray->field(0));
        UNIT_ASSERT_EQUAL(static_cast<ui64>(listArray->length()), values.size());

        UNIT_ASSERT_EQUAL(wrapArray->field(1)->type_id(), arrow::Type::UINT64);
        auto customArray = static_pointer_cast<arrow::UInt64Array>(wrapArray->field(1));
        UNIT_ASSERT_EQUAL(static_cast<ui64>(customArray->length()), values.size());

        UNIT_ASSERT_EQUAL(listArray->value_type()->id(), arrow::Type::STRUCT);
        auto structArray = static_pointer_cast<arrow::StructArray>(listArray->values());

        UNIT_ASSERT_EQUAL(listArray->num_fields(), 1);
        UNIT_ASSERT_EQUAL(structArray->num_fields(), 2);
        UNIT_ASSERT_EQUAL(structArray->field(0)->type_id(), arrow::Type::DOUBLE);
        UNIT_ASSERT_EQUAL(structArray->field(1)->type_id(), arrow::Type::STRUCT);
        auto keysArray = static_pointer_cast<arrow::DoubleArray>(structArray->field(0));
        auto itemsArray = static_pointer_cast<arrow::StructArray>(structArray->field(1));
        UNIT_ASSERT_EQUAL(itemsArray->num_fields(), 2);
        UNIT_ASSERT_EQUAL(itemsArray->field(0)->type_id(), arrow::Type::INT32);
        UNIT_ASSERT_EQUAL(itemsArray->field(1)->type_id(), arrow::Type::UINT32);
        auto i32Array = static_pointer_cast<arrow::Int32Array>(itemsArray->field(0));
        auto ui32Array = static_pointer_cast<arrow::UInt32Array>(itemsArray->field(1));

        ui64 index = 0;
        for (const auto& value: values) {
            UNIT_ASSERT(value.GetDictLength() == static_cast<ui64>(listArray->value_length(index)));
            for (auto subindex = listArray->value_offset(index); subindex < listArray->value_offset(index + 1); ++subindex) {
                NUdf::TUnboxedValue key = keysArray->IsNull(subindex)
                                        ? NUdf::TUnboxedValuePod()
                                        : NUdf::TUnboxedValuePod(keysArray->Value(subindex));
                UNIT_ASSERT(value.Contains(key));
                NUdf::TUnboxedValue payloadValue = value.Lookup(key);
                UNIT_ASSERT_EQUAL(payloadValue.GetElement(0).Get<i32>(), i32Array->Value(subindex));
                UNIT_ASSERT_EQUAL(payloadValue.GetElement(1).Get<ui32>(), ui32Array->Value(subindex));
            }
            ++index;
        }
    }

    Y_UNIT_TEST(OptionalOfOptional) {
        TTestContext context;

        auto doubleOptionalType = context.GetOptionalOfOptionalType();
        UNIT_ASSERT(IsArrowCompatible(doubleOptionalType));

        auto values = context.CreateOptionalOfOptional(100);
        auto array = MakeArrowArray(values, doubleOptionalType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT_EQUAL(static_cast<ui64>(array->length()), values.size());

        auto index = 0;
        for (auto value: values) {
            std::shared_ptr<arrow::Array> currentArray = array;
            int depth = 0;

            while (currentArray->type()->id() == arrow::Type::STRUCT) {
                auto structArray = static_pointer_cast<arrow::StructArray>(currentArray);
                UNIT_ASSERT_EQUAL(structArray->num_fields(), 1);

                if (structArray->IsNull(index)) {
                    break;
                }

                ++depth;

                auto childArray = structArray->field(0);
                if (childArray->type()->id() == arrow::Type::DENSE_UNION) {
                    break;
                }

                currentArray = childArray;
            }

            while (depth--) {
                UNIT_ASSERT(value);
                value = value.GetOptionalValue();
            }

            if (value.HasValue()) {
                if (currentArray->type()->id() == arrow::Type::INT32) {
                    UNIT_ASSERT_EQUAL(value.Get<i32>(), static_pointer_cast<arrow::Int32Array>(currentArray)->Value(index));
                } else {
                    UNIT_ASSERT(!currentArray->IsNull(index));
                }
            } else {
                UNIT_ASSERT(currentArray->IsNull(index));
            }

            ++index;
        }
    }

    Y_UNIT_TEST(LargeVariant) {
        TTestContext context;

        ui32 numberOfTypes = 500;
        auto variantType = context.GetLargeVariantType(numberOfTypes);
        UNIT_ASSERT(IsArrowCompatible(variantType));

        auto values = context.CreateLargeVariant(1000);
        auto array = MakeArrowArray(values, variantType);
        UNIT_ASSERT(array->ValidateFull().ok());
        UNIT_ASSERT_EQUAL(static_cast<ui64>(array->length()), values.size());
        UNIT_ASSERT_EQUAL(array->type_id(), arrow::Type::DENSE_UNION);
        auto unionArray = static_pointer_cast<arrow::DenseUnionArray>(array);
        ui32 numberOfGroups = (numberOfTypes - 1) / arrow::UnionType::kMaxTypeCode + 1;
        UNIT_ASSERT_EQUAL(numberOfGroups, static_cast<ui32>(unionArray->num_fields()));
        ui32 typesInArrow = 0;
        for (auto i = 0 ; i < unionArray->num_fields(); ++i) {
            UNIT_ASSERT_EQUAL(unionArray->field(i)->type_id(), arrow::Type::DENSE_UNION);
            typesInArrow += unionArray->field(i)->num_fields();
        }
        UNIT_ASSERT_EQUAL(numberOfTypes, typesInArrow);
    }
}

Y_UNIT_TEST_SUITE(ConvertUnboxedValueToArrowAndBack){
    Y_UNIT_TEST(OptionalListOfOptional) {
        TTestContext context;

        auto listType = context.GetOptionalListOfOptional();
        Y_ABORT_UNLESS(IsArrowCompatible(listType));

        auto values = context.CreateOptionalListOfOptional(100);
        auto array = MakeArrowArray(values, listType);
        auto restoredValues = ExtractUnboxedVector(array, listType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], listType);
        }
    }

    // Y_UNIT_TEST(VariantOverStruct) {
    //     TTestContext context;

    //     auto variantType = context.GetVariantOverStructType();
    //     UNIT_ASSERT(IsArrowCompatible(variantType));

    //     auto values = context.CreateVariantOverStruct(100);
    //     auto array = MakeArrowArray(values, variantType);
    //     auto restoredValues = ExtractUnboxedVector(array, variantType, context.HolderFactory);
    //     UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
    //     for (ui64 index = 0; index < values.size(); ++index) {
    //         AssertUnboxedValuesAreEqual(values[index], restoredValues[index], variantType);
    //     }
    // }

    // Y_UNIT_TEST(OptionalVariantOverStruct) {
    //     TTestContext context;

    //     auto optionalVariantType = context.GetOptionalVariantOverStructType();
    //     UNIT_ASSERT(IsArrowCompatible(optionalVariantType));

    //     auto values = context.CreateOptionalVariantOverStruct(100);
    //     auto array = MakeArrowArray(values, optionalVariantType);
    //     auto restoredValues = ExtractUnboxedVector(array, optionalVariantType, context.HolderFactory);
    //     UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
    //     for (ui64 index = 0; index < values.size(); ++index) {
    //         AssertUnboxedValuesAreEqual(values[index], restoredValues[index], optionalVariantType);
    //     }
    // }

    // Y_UNIT_TEST(DoubleOptionalVariantOverStruct) {
    //     TTestContext context;

    //     auto doubleOptionalVariantType = context.GetDoubleOptionalVariantOverStructType();
    //     UNIT_ASSERT(IsArrowCompatible(doubleOptionalVariantType));

    //     auto values = context.CreateDoubleOptionalVariantOverStruct(100);
    //     auto array = MakeArrowArray(values, doubleOptionalVariantType);
    //     auto restoredValues = ExtractUnboxedVector(array, doubleOptionalVariantType, context.HolderFactory);
    //     UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
    //     for (ui64 index = 0; index < values.size(); ++index) {
    //         AssertUnboxedValuesAreEqual(values[index], restoredValues[index], doubleOptionalVariantType);
    //     }
    // }

    Y_UNIT_TEST(VariantOverTupleWithOptionals) {
        TTestContext context;

        auto variantType = context.GetVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(IsArrowCompatible(variantType));

        auto values = context.CreateVariantOverTupleWithOptionals(100);
        auto array = MakeArrowArray(values, variantType);
        auto restoredValues = ExtractUnboxedVector(array, variantType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], variantType);
        }
    }

    Y_UNIT_TEST(OptionalVariantOverTupleWithOptionals) {
        TTestContext context;

        auto optionalVariantType = context.GetOptionalVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(IsArrowCompatible(optionalVariantType));

        auto values = context.CreateOptionalVariantOverTupleWithOptionals(100);
        auto array = MakeArrowArray(values, optionalVariantType);
        auto restoredValues = ExtractUnboxedVector(array, optionalVariantType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], optionalVariantType);
        }
    }

    Y_UNIT_TEST(DoubleOptionalVariantOverTupleWithOptionals) {
        TTestContext context;

        auto doubleOptionalVariantType = context.GetDoubleOptionalVariantOverTupleWithOptionalsType();
        UNIT_ASSERT(IsArrowCompatible(doubleOptionalVariantType));

        auto values = context.CreateDoubleOptionalVariantOverTupleWithOptionals(100);
        auto array = MakeArrowArray(values, doubleOptionalVariantType);
        auto restoredValues = ExtractUnboxedVector(array, doubleOptionalVariantType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], doubleOptionalVariantType);
        }
    }

    Y_UNIT_TEST(DictOptionalToTuple) {
        TTestContext context;

        auto dictType = context.GetDictOptionalToTupleType();
        UNIT_ASSERT(IsArrowCompatible(dictType));

        auto values = context.CreateDictOptionalToTuple(100);
        auto array = MakeArrowArray(values, dictType);
        auto restoredValues = ExtractUnboxedVector(array, dictType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], dictType);
        }
    }

    Y_UNIT_TEST(OptionalOfOptional) {
        TTestContext context;

        auto doubleOptionalType = context.GetOptionalOfOptionalType();
        UNIT_ASSERT(IsArrowCompatible(doubleOptionalType));

        auto values = context.CreateOptionalOfOptional(100);
        auto array = MakeArrowArray(values, doubleOptionalType);
        auto restoredValues = ExtractUnboxedVector(array, doubleOptionalType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], doubleOptionalType);
        }
    }

    Y_UNIT_TEST(LargeVariant) {
        TTestContext context;

        auto variantType = context.GetLargeVariantType(500);
        UNIT_ASSERT(IsArrowCompatible(variantType));

        auto values = context.CreateLargeVariant(1000);
        auto array = MakeArrowArray(values, variantType);
        auto restoredValues = ExtractUnboxedVector(array, variantType, context.HolderFactory);
        UNIT_ASSERT_EQUAL(values.size(), restoredValues.size());
        for (ui64 index = 0; index < values.size(); ++index) {
            AssertUnboxedValuesAreEqual(values[index], restoredValues[index], variantType);
        }
    }
}

} // namespace NKikimr::NKqp::NFormats

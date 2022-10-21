#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/public/udf/udf_types.h>

#include <library/cpp/presort/presort.h>

#include <util/random/random.h>
#include <util/datetime/cputimer.h>
#include <util/string/builder.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

struct TSettings {
    ui32 Index;
    bool IsOptional;
    NKikimr::NUdf::EDataSlot Slot;
};

template <bool Desc>
struct TPresortOps : public NPresort::TResultOps {
    const TVector<TSettings>& Settings;
    NUdf::TUnboxedValue* Items;

    size_t Current = 0;

    TPresortOps(
        const TVector<TSettings>& settings,
        NUdf::TUnboxedValue* items)
        : Settings(settings)
        , Items(items)
    {}

    void Encode(IOutputStream& out) {
        for (const auto& setting : Settings) {
            auto& value = Items[setting.Index];

            switch (setting.Slot) {
            case NUdf::EDataSlot::Bool:
                NPresort::EncodeUnsignedInt(out, value.template Get<bool>(), Desc);
                break;
            case NUdf::EDataSlot::Uint8:
                NPresort::EncodeUnsignedInt(out, value.template Get<ui8>(), Desc);
                break;
            case NUdf::EDataSlot::Uint16:
            case NUdf::EDataSlot::Date:
                NPresort::EncodeUnsignedInt(out, value.template Get<ui16>(), Desc);
                break;
            case NUdf::EDataSlot::Uint32:
            case NUdf::EDataSlot::Datetime:
                NPresort::EncodeUnsignedInt(out, value.template Get<ui32>(), Desc);
                break;
            case NUdf::EDataSlot::Uint64:
            case NUdf::EDataSlot::Timestamp:
                NPresort::EncodeUnsignedInt(out, value.template Get<ui64>(), Desc);
                break;
            case NUdf::EDataSlot::Int8:
                NPresort::EncodeSignedInt(out, value.template Get<i8>(), Desc);
                break;
            case NUdf::EDataSlot::Int16:
                NPresort::EncodeSignedInt(out, value.template Get<i16>(), Desc);
                break;
            case NUdf::EDataSlot::Int32:
                NPresort::EncodeSignedInt(out, value.template Get<i32>(), Desc);
                break;
            case NUdf::EDataSlot::Int64:
            case NUdf::EDataSlot::Interval:
                NPresort::EncodeSignedInt(out, value.template Get<i64>(), Desc);
                break;
            case NUdf::EDataSlot::Float:
                NPresort::EncodeFloating(out, value.template Get<float>(), Desc);
                break;
            case NUdf::EDataSlot::Double:
                NPresort::EncodeFloating(out, value.template Get<double>(), Desc);
                break;
            case NUdf::EDataSlot::String:
            case NUdf::EDataSlot::Utf8: {
                auto strRef = value.AsStringRef();
                NPresort::EncodeString(out, TStringBuf(strRef.Data(), strRef.Size()), Desc);
                break;
            }
            default:
                MKQL_ENSURE(false, TStringBuilder() << "Unknown slot: " << setting.Slot);
            }
        }
    }

    void SetError(const TString& err) {
        MKQL_ENSURE(false, TStringBuilder() << "Presort decoding error: " << err);
    }

    void SetUnsignedInt(ui64 value) {
        const auto& setting = Settings[Current++];
        switch (setting.Slot) {
        case NUdf::EDataSlot::Bool:
            Items[setting.Index] = NUdf::TUnboxedValuePod(value != 0);
            break;
        case NUdf::EDataSlot::Uint8:
            Items[setting.Index] = NUdf::TUnboxedValuePod(static_cast<ui8>(value));
            break;
        case NUdf::EDataSlot::Uint16:
        case NUdf::EDataSlot::Date:
            Items[setting.Index] = NUdf::TUnboxedValuePod(static_cast<ui16>(value));
            break;
        case NUdf::EDataSlot::Uint32:
        case NUdf::EDataSlot::Datetime:
            Items[setting.Index] = NUdf::TUnboxedValuePod(static_cast<ui32>(value));
            break;
        case NUdf::EDataSlot::Uint64:
        case NUdf::EDataSlot::Timestamp:
            Items[setting.Index] = NUdf::TUnboxedValuePod(value);
            break;
        default:
            MKQL_ENSURE(false, TStringBuilder() << "Unknown slot: " << setting.Slot);
        }
    }

    void SetSignedInt(i64 value) {
        const auto& setting = Settings[Current++];
        switch (setting.Slot) {
        case NUdf::EDataSlot::Int8:
            Items[setting.Index] = NUdf::TUnboxedValuePod(static_cast<i8>(value));
            break;
        case NUdf::EDataSlot::Int16:
            Items[setting.Index] = NUdf::TUnboxedValuePod(static_cast<i16>(value));
            break;
        case NUdf::EDataSlot::Int32:
            Items[setting.Index] = NUdf::TUnboxedValuePod(static_cast<i32>(value));
            break;
        case NUdf::EDataSlot::Int64:
        case NUdf::EDataSlot::Interval:
            Items[setting.Index] = NUdf::TUnboxedValuePod(value);
            break;
        default:
            MKQL_ENSURE(false, "Unknown slot: " << setting.Slot);
        }
    }

    void SetFloat(float value) {
        Items[Settings[Current++].Index] = NUdf::TUnboxedValuePod(value);
    }

    void SetDouble(double value) {
        Items[Settings[Current++].Index] = NUdf::TUnboxedValuePod(value);
    }

    void SetString(const TString& value) {
        Items[Settings[Current++].Index] = MakeString(NUdf::TStringRef(value.data(), value.size()));
    }

    void SetOptional(bool) {}
};

template <typename T>
NUdf::TUnboxedValue RandomValue() {
    return NUdf::TUnboxedValuePod(RandomNumber<T>());
}

template <>
NUdf::TUnboxedValue RandomValue<char*>() {
    auto length = RandomNumber<ui64>(64);
    return MakeStringNotFilled(length);
}

template <typename T, NUdf::EDataSlot Slot, bool Desc>
std::pair<ui64, ui64> MeasureOld() {
    constexpr size_t count = 1000;
    constexpr size_t rowCount = 100000;

    TScopedAlloc alloc(__LOCATION__);
    TMemoryUsageInfo memInfo("Memory");

    TVector<NUdf::TUnboxedValue> values;
    TVector<TSettings> settings;

    for (ui32 i = 0; i < count; ++i) {
        values.push_back(RandomValue<T>());
        settings.push_back({i, false, NUdf::TDataType<T>::Slot});
    }

    TSimpleTimer timer;

    TStringStream stream;
    for (size_t n = 0; n < rowCount; ++n) {
        stream.clear();
        TPresortOps<Desc> ops{settings, values.begin()};
        ops.Encode(stream);
    }
    auto encodeTime = timer.Get().MicroSeconds();

    auto rowSize = stream.Str().size();
    Cerr << "row size " << rowSize << ", row count " << rowCount << Endl;
    Cerr << "encoding " << rowSize * rowCount * 1000000 / encodeTime << " bytes per sec ("
        << encodeTime << " us)" << Endl;

    timer.Reset();
    for (size_t n = 0; n < rowCount; ++n) {
        TPresortOps<Desc> ops{settings, values.begin()};
        auto str = stream.Str();
        NPresort::Decode(ops, TStringBuf(str.data(), str.size()));
    }
    auto decodeTime = timer.Get().MicroSeconds();

    Cerr << "decoding " << rowSize * rowCount * 1000000 / decodeTime << " bytes per sec ("
        << decodeTime << " us)" << Endl;
    Cerr << Endl;

    return std::make_pair(encodeTime, decodeTime);
}

template <typename T, NUdf::EDataSlot Slot, bool Desc>
std::pair<ui64, ui64> MeasureNew() {
    constexpr size_t count = 1000;
    constexpr size_t rowCount = 100000;

    TScopedAlloc alloc(__LOCATION__);
    TMemoryUsageInfo memInfo("Memory");

    TVector<NUdf::TUnboxedValuePod> values;
    TPresortEncoder encoder;
    TPresortDecoder decoder;

    for (size_t i = 0; i < count; ++i) {
        values.push_back(RandomValue<T>());
        encoder.AddType(Slot, false, Desc);
        decoder.AddType(Slot, false, Desc);
    }

    TSimpleTimer timer;
    TStringBuf buffer;

    for (size_t n = 0; n < rowCount; ++n) {
        encoder.Start();
        for (size_t i = 0; i < count; ++i) {
            encoder.Encode(values[i]);
        }
        buffer = encoder.Finish();
    }
    auto encodeTime = timer.Get().MicroSeconds();

    auto rowSize = buffer.size();

    Cerr << "row size " << rowSize << ", row count " << rowCount << Endl;
    Cerr << "encoding " << rowSize * rowCount * 1000000 / encodeTime << " bytes per sec ("
        << encodeTime << " us)" << Endl;

    timer.Reset();
    for (size_t n = 0; n < rowCount; ++n) {
        decoder.Start(buffer);
        for (size_t i = 0; i < count; ++i) {
            decoder.Decode();
        }
        encoder.Finish();
    }
    auto decodeTime = timer.Get().MicroSeconds();

    Cerr << "decoding " << rowSize * rowCount * 1000000 / decodeTime << " bytes per sec ("
        << decodeTime << " us)" << Endl;
    Cerr << Endl;

    return std::make_pair(encodeTime, decodeTime);
}

template <typename T, NUdf::EDataSlot Slot, bool Desc>
void Compare() {
    auto newTimes = MeasureNew<T, Slot, Desc>();
    auto oldTimes = MeasureOld<T, Slot, Desc>();

    Cerr << "encoding speedup " << (double)oldTimes.first / (double)newTimes.first << Endl;
    Cerr << "decoding speedup " << (double)oldTimes.second / (double)newTimes.second << Endl;
    Cerr << "--------" << Endl << Endl;
}

template <typename T, NUdf::EDataSlot Slot>
void CompareType(const char* type) {
    Cerr << type << Endl;
    Compare<T, Slot, false>();

    Cerr << type << " desc" << Endl;
    Compare<T, Slot, true>();
}

}

int main(int, char**) {
    CompareType<bool, NUdf::EDataSlot::Bool>("bool");
    CompareType<ui8, NUdf::EDataSlot::Uint8>("ui8");
    CompareType<ui16, NUdf::EDataSlot::Uint16>("ui16");
    CompareType<ui32, NUdf::EDataSlot::Uint32>("ui32");
    CompareType<ui64, NUdf::EDataSlot::Uint64>("ui64");
    CompareType<float, NUdf::EDataSlot::Float>("float");
    CompareType<double, NUdf::EDataSlot::Double>("double");
    CompareType<char*, NUdf::EDataSlot::String>("string");

    return 0;
}

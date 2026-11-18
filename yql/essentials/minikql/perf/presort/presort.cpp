#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/computation/presort.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/public/udf/udf_types.h>

#include <library/cpp/presort/presort.h>

#include <util/random/random.h>
#include <benchmark/benchmark.h>
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
struct TPresortOps: public NPresort::TResultOps {
    const TVector<TSettings>& Settings;
    NUdf::TUnboxedValue* Items;

    size_t Current = 0;

    TPresortOps(
        const TVector<TSettings>& settings,
        NUdf::TUnboxedValue* items)
        : Settings(settings)
        , Items(items)
    {
    }

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

    void SetOptional(bool) {
    }
};

template <typename T>
NUdf::TUnboxedValue RandomValue() {
    return NUdf::TUnboxedValuePod(RandomNumber<T>());
}

template <>
NUdf::TUnboxedValue RandomValue<char*>() {
    auto length = RandomNumber<ui64>(64);
    TString data(length, '\0');
    for (auto& byte : data) {
        byte = static_cast<char>(RandomNumber<ui8>());
    }
    return MakeString(NUdf::TStringRef(data.data(), data.size()));
}

enum class EImplementation {
    Legacy,
    Modern,
};
enum class EOperation {
    Decode,
    Encode,
};

constexpr size_t ColumnCount = 1000;

template <typename T, NUdf::EDataSlot Slot, bool Desc>
struct TModernFixture {
    TScopedAlloc Alloc{__LOCATION__};
    TMemoryUsageInfo MemInfo{"Memory"};
    TVector<NUdf::TUnboxedValue> Values;
    TPresortEncoder Encoder;
    TPresortDecoder Decoder;
    TStringBuf Buffer;

    TModernFixture() {
        for (size_t i = 0; i < ColumnCount; ++i) {
            Values.push_back(RandomValue<T>());
            Encoder.AddType(Slot, /*isOptional=*/false, Desc);
            Decoder.AddType(Slot, /*isOptional=*/false, Desc);
        }
    }

    void Encode() {
        Encoder.Start();
        for (const auto& value : Values) {
            Encoder.Encode(value);
        }
        Buffer = Encoder.Finish();
    }

    void Observe() {
        benchmark::DoNotOptimize(Buffer);
    }

    void Decode() {
        Decoder.Start(Buffer);
        for (size_t i = 0; i < ColumnCount; ++i) {
            Decoder.Decode();
        }
        Decoder.Finish();
    }
};

template <typename T, bool Desc>
struct TLegacyFixture {
    TScopedAlloc Alloc{__LOCATION__};
    TMemoryUsageInfo MemInfo{"Memory"};
    TVector<NUdf::TUnboxedValue> Values;
    TVector<TSettings> Settings;
    TStringStream Stream;

    TLegacyFixture() {
        for (ui32 i = 0; i < ColumnCount; ++i) {
            Values.push_back(RandomValue<T>());
            Settings.push_back({i, false, NUdf::TDataType<T>::Slot});
        }
    }

    void Encode() {
        Stream.clear();
        TPresortOps<Desc> ops{Settings, Values.begin()};
        ops.Encode(Stream);
    }

    void Observe() {
        benchmark::DoNotOptimize(Stream.Str());
    }

    void Decode() {
        TPresortOps<Desc> ops{Settings, Values.begin()};
        const auto& str = Stream.Str();
        NPresort::Decode(ops, TStringBuf(str.data(), str.size()));
    }
};

template <typename T, NUdf::EDataSlot Slot, bool Desc, EImplementation Implementation, EOperation Operation>
void PresortBenchmark(benchmark::State& state) {
    using TFixture = std::conditional_t<Implementation == EImplementation::Modern, TModernFixture<T, Slot, Desc>, TLegacyFixture<T, Desc>>;
    TFixture fixture;
    if constexpr (Operation == EOperation::Decode) {
        fixture.Encode();
    }
    for (auto _ : state) {
        if constexpr (Operation == EOperation::Encode) {
            fixture.Encode();
        } else {
            fixture.Decode();
        }
        fixture.Observe();
    }
}

#define REGISTER_PRESORT_BENCHMARKS(Type, Slot)                                                           \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, false, EImplementation::Modern, EOperation::Encode); \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, false, EImplementation::Modern, EOperation::Decode); \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, false, EImplementation::Legacy, EOperation::Encode); \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, false, EImplementation::Legacy, EOperation::Decode); \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, true, EImplementation::Modern, EOperation::Encode);  \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, true, EImplementation::Modern, EOperation::Decode);  \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, true, EImplementation::Legacy, EOperation::Encode);  \
    BENCHMARK_TEMPLATE(PresortBenchmark, Type, Slot, true, EImplementation::Legacy, EOperation::Decode)

REGISTER_PRESORT_BENCHMARKS(bool, NUdf::EDataSlot::Bool);
REGISTER_PRESORT_BENCHMARKS(ui8, NUdf::EDataSlot::Uint8);
REGISTER_PRESORT_BENCHMARKS(ui16, NUdf::EDataSlot::Uint16);
REGISTER_PRESORT_BENCHMARKS(ui32, NUdf::EDataSlot::Uint32);
REGISTER_PRESORT_BENCHMARKS(ui64, NUdf::EDataSlot::Uint64);
REGISTER_PRESORT_BENCHMARKS(float, NUdf::EDataSlot::Float);
REGISTER_PRESORT_BENCHMARKS(double, NUdf::EDataSlot::Double);
REGISTER_PRESORT_BENCHMARKS(char*, NUdf::EDataSlot::String);

#undef REGISTER_PRESORT_BENCHMARKS

} // namespace

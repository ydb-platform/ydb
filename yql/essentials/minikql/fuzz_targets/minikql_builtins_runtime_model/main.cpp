#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/minikql/mkql_terminator.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <array>
#include <cstddef>
#include <string_view>
#include <utility>

namespace NKikimr::NMiniKQL {
namespace {

constexpr ui32 MaxProgramParts = 8;
constexpr ui32 MaxListItems = 8;
constexpr ui32 MaxStringBytes = 24;

volatile size_t ValueSink = 0;

class TMiniKqlRuntime {
public:
    TMiniKqlRuntime()
        : Alloc_(__LOCATION__)
        , FunctionRegistry_(CreateFunctionRegistry(CreateBuiltinRegistry()))
        , RandomProvider_(CreateDeterministicRandomProvider(1))
        , TimeProvider_(CreateDeterministicTimeProvider(10000000))
        , StatsRegistry_(CreateDefaultStatsRegistry())
        , RuntimeSettings_(NYql::MakeRuntimeSettingsMutable())
        , Env_(Alloc_)
        , Builder_(Env_, *FunctionRegistry_)
    {
    }

    TProgramBuilder& Builder() {
        return Builder_;
    }

    void Execute(TRuntimeNode program) {
        TExploringNodeVisitor explorer;
        explorer.Walk(program.GetNode(), Env_.GetNodeStack());

        TComputationPatternOpts opts(
            Alloc_.Ref(),
            Env_,
            GetBuiltinFactory(),
            FunctionRegistry_.Get(),
            NUdf::EValidateMode::Greedy,
            NUdf::EValidatePolicy::Exception,
            "OFF",
            EGraphPerProcess::Multi,
            StatsRegistry_.Get(),
            nullptr,
            nullptr,
            nullptr,
            NYql::UnknownLangVersion,
            RuntimeSettings_);

        const auto pattern = MakeComputationPattern(explorer, program, {}, opts);
        const auto graph = pattern->Clone(opts.ToComputationOptions(*RandomProvider_, *TimeProvider_));
        TBindTerminator terminator(graph->GetTerminator());

        const auto value = graph->GetValue();
        if (value) {
            ValueSink += static_cast<size_t>(value.Get<i64>() & 0xff);
        }
    }

private:
    TScopedAlloc Alloc_;
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry_;
    TIntrusivePtr<IRandomProvider> RandomProvider_;
    TIntrusivePtr<ITimeProvider> TimeProvider_;
    IStatsRegistryPtr StatsRegistry_;
    NYql::TRuntimeSettings::TPtr RuntimeSettings_;
    TTypeEnvironment Env_;
    TProgramBuilder Builder_;
};

class TModelBuilder {
public:
    TModelBuilder(TProgramBuilder& pb, FuzzedDataProvider& fdp)
        : Pb_(pb)
        , Fdp_(fdp)
        , I64Type_(Pb_.NewDataType(NUdf::EDataSlot::Int64))
        , StringType_(Pb_.NewDataType(NUdf::EDataSlot::String))
        , JsonType_(Pb_.NewDataType(NUdf::EDataSlot::Json))
        , YsonType_(Pb_.NewDataType(NUdf::EDataSlot::Yson))
    {
    }

    TRuntimeNode BuildProgram() {
        const ui32 partCount = Fdp_.ConsumeIntegralInRange<ui32>(1, MaxProgramParts);
        TRuntimeNode acc = Pb_.NewDataLiteral<i64>(0);

        for (ui32 i = 0; i < partCount; ++i) {
            TRuntimeNode part;
            switch (Fdp_.ConsumeIntegralInRange<ui8>(0, 4)) {
                case 0:
                    part = BuildNumericModel();
                    break;
                case 1:
                    part = BuildStringModel();
                    break;
                case 2:
                    part = BuildDictModel();
                    break;
                case 3:
                    part = BuildOptionalStructModel();
                    break;
                default:
                    part = BuildJsonLikeModel();
                    break;
            }
            acc = Pb_.Add(acc, part);
        }

        return acc;
    }

private:
    TRuntimeNode I64(i64 min = -10000, i64 max = 10000) {
        return Pb_.NewDataLiteral<i64>(Fdp_.ConsumeIntegralInRange<i64>(min, max));
    }

    TRuntimeNode NonZeroI64() {
        i64 value = Fdp_.ConsumeIntegralInRange<i64>(-4096, 4096);
        if (value == 0) {
            value = 1;
        }
        return Pb_.NewDataLiteral<i64>(value);
    }

    TRuntimeNode Ui64(ui64 max = 10000) {
        return Pb_.NewDataLiteral<ui64>(Fdp_.ConsumeIntegralInRange<ui64>(0, max));
    }

    TRuntimeNode Bool() {
        return Pb_.NewDataLiteral<bool>(Fdp_.ConsumeBool());
    }

    TString SmallString() {
        TString s(Fdp_.ConsumeRandomLengthString(MaxStringBytes));
        for (char& c : s) {
            if (static_cast<unsigned char>(c) < 0x20) {
                c = static_cast<char>('a' + (static_cast<unsigned char>(c) % 26));
            }
        }
        return s;
    }

    TRuntimeNode StringLiteral(const TString& s) {
        return Pb_.NewDataLiteral<NUdf::EDataSlot::String>(NUdf::TStringRef(s.data(), s.size()));
    }

    TRuntimeNode SmallStringLiteral() {
        return StringLiteral(SmallString());
    }

    TRuntimeNode ToI64(TRuntimeNode value) {
        return Pb_.ToIntegral(value, I64Type_);
    }

    TRuntimeNode BoolScore(TRuntimeNode value) {
        return Pb_.If(value, Pb_.NewDataLiteral<i64>(1), Pb_.NewDataLiteral<i64>(0));
    }

    TRuntimeNode LengthScore(TRuntimeNode listOrDict) {
        return ToI64(Pb_.Length(listOrDict));
    }

    TRuntimeNode SizeScore(TRuntimeNode stringValue) {
        return ToI64(Pb_.Size(stringValue));
    }

    TRuntimeNode AddAll(TVector<TRuntimeNode> values) {
        TRuntimeNode acc = Pb_.NewDataLiteral<i64>(0);
        for (const auto& value : values) {
            acc = Pb_.Add(acc, value);
        }
        return acc;
    }

    TRuntimeNode BuildI64List() {
        TVector<TRuntimeNode> items;
        const ui32 count = Fdp_.ConsumeIntegralInRange<ui32>(0, MaxListItems);
        items.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            items.push_back(I64(-2048, 2048));
        }
        return Pb_.NewList(I64Type_, items);
    }

    TRuntimeNode BuildStringList() {
        TVector<TRuntimeNode> items;
        const ui32 count = Fdp_.ConsumeIntegralInRange<ui32>(0, MaxListItems);
        items.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            items.push_back(SmallStringLiteral());
        }
        return Pb_.NewList(StringType_, items);
    }

    TRuntimeNode BuildStructList() {
        auto structType = Pb_.NewStructType(Pb_.NewEmptyStructType(), "Key", I64Type_);
        structType = Pb_.NewStructType(structType, "Payload", StringType_);

        TVector<TRuntimeNode> items;
        const ui32 count = Fdp_.ConsumeIntegralInRange<ui32>(0, MaxListItems);
        items.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            TVector<std::pair<std::string_view, TRuntimeNode>> members = {
                {"Key", I64(-32, 32)},
                {"Payload", SmallStringLiteral()},
            };
            items.push_back(Pb_.NewStruct(structType, members));
        }
        return Pb_.NewList(structType, items);
    }

    TRuntimeNode BuildNumericModel() {
        const auto lhs = I64(-4096, 4096);
        const auto rhs = NonZeroI64();
        TRuntimeNode arithmetic;

        switch (Fdp_.ConsumeIntegralInRange<ui8>(0, 7)) {
            case 0:
                arithmetic = Pb_.Add(lhs, rhs);
                break;
            case 1:
                arithmetic = Pb_.Sub(lhs, rhs);
                break;
            case 2:
                arithmetic = Pb_.Mul(lhs, rhs);
                break;
            case 3:
                arithmetic = Pb_.Div(lhs, rhs);
                break;
            case 4:
                arithmetic = Pb_.Mod(lhs, rhs);
                break;
            case 5:
                arithmetic = Pb_.Min(lhs, rhs);
                break;
            case 6:
                arithmetic = Pb_.Max(lhs, rhs);
                break;
            default:
                arithmetic = Pb_.Abs(lhs);
                break;
        }

        const auto bitLhs = Ui64(0xffff);
        const auto bitRhs = Ui64(63);
        TRuntimeNode bits;
        switch (Fdp_.ConsumeIntegralInRange<ui8>(0, 5)) {
            case 0:
                bits = Pb_.BitAnd(bitLhs, bitRhs);
                break;
            case 1:
                bits = Pb_.BitOr(bitLhs, bitRhs);
                break;
            case 2:
                bits = Pb_.BitXor(bitLhs, bitRhs);
                break;
            case 3:
                bits = Pb_.ShiftLeft(bitLhs, Pb_.NewDataLiteral<ui8>(Fdp_.ConsumeIntegralInRange<ui8>(0, 7)));
                break;
            case 4:
                bits = Pb_.ShiftRight(bitLhs, Pb_.NewDataLiteral<ui8>(Fdp_.ConsumeIntegralInRange<ui8>(0, 7)));
                break;
            default:
                bits = Pb_.CountBits(bitLhs);
                break;
        }

        const auto list = BuildI64List();
        const i64 addend = Fdp_.ConsumeIntegralInRange<i64>(-16, 16);
        const i64 limit = Fdp_.ConsumeIntegralInRange<i64>(-2048, 2048);
        const auto mapped = Pb_.Map(list, [&](TRuntimeNode item) {
            return Pb_.Add(item, Pb_.NewDataLiteral<i64>(addend));
        });
        const auto filtered = Pb_.Filter(mapped, [&](TRuntimeNode item) {
            return Pb_.Less(item, Pb_.NewDataLiteral<i64>(limit));
        });
        const auto taken = Pb_.Take(filtered, Ui64(MaxListItems));

        TVector<TRuntimeNode> scores = {
            arithmetic,
            BoolScore(Pb_.GreaterOrEqual(arithmetic, I64(-4096, 4096))),
            ToI64(bits),
            LengthScore(taken),
            LengthScore(Pb_.Sort(taken, Bool(), [](TRuntimeNode item) { return item; })),
        };
        return AddAll(scores);
    }

    TRuntimeNode BuildStringModel() {
        const auto lhs = SmallStringLiteral();
        const auto rhs = SmallStringLiteral();
        const auto combined = Pb_.Concat(lhs, rhs);
        const auto start = Pb_.NewDataLiteral<ui32>(Fdp_.ConsumeIntegralInRange<ui32>(0, MaxStringBytes));
        const auto count = Pb_.NewDataLiteral<ui32>(Fdp_.ConsumeIntegralInRange<ui32>(0, MaxStringBytes));
        const auto pos = Pb_.NewDataLiteral<ui32>(Fdp_.ConsumeIntegralInRange<ui32>(0, MaxStringBytes));
        const auto index = Pb_.NewDataLiteral<ui32>(Fdp_.ConsumeIntegralInRange<ui32>(0, MaxStringBytes * 2));

        const auto strings = BuildStringList();
        const auto sizes = Pb_.Map(strings, [&](TRuntimeNode item) {
            return Pb_.Size(item);
        });

        TVector<TRuntimeNode> scores = {
            SizeScore(Pb_.Substring(combined, start, count)),
            BoolScore(Pb_.Exists(Pb_.Find(combined, rhs, pos))),
            BoolScore(Pb_.Exists(Pb_.RFind(combined, lhs, pos))),
            BoolScore(Pb_.StartsWith(combined, lhs)),
            BoolScore(Pb_.EndsWith(combined, rhs)),
            BoolScore(Pb_.StringContains(combined, rhs)),
            BoolScore(Pb_.Exists(Pb_.ByteAt(combined, index))),
            LengthScore(sizes),
        };
        return AddAll(scores);
    }

    TRuntimeNode BuildDictModel() {
        TVector<std::pair<TRuntimeNode, TRuntimeNode>> dictItems;
        const ui32 count = Fdp_.ConsumeIntegralInRange<ui32>(0, MaxListItems);
        dictItems.reserve(count);
        for (ui32 i = 0; i < count; ++i) {
            dictItems.emplace_back(I64(-32, 32), SmallStringLiteral());
        }

        const auto dictType = Pb_.NewDictType(I64Type_, StringType_, false);
        const auto directDict = Pb_.NewDict(dictType, dictItems);
        const auto keys = BuildI64List();
        const auto contains = Pb_.Map(keys, [&](TRuntimeNode key) {
            return Pb_.Contains(directDict, key);
        });
        const auto lookup = Pb_.Map(keys, [&](TRuntimeNode key) {
            return Pb_.Lookup(directDict, key);
        });

        const auto rows = BuildStructList();
        const auto materializedDict = Fdp_.ConsumeBool()
            ? Pb_.ToHashedDict(rows, Fdp_.ConsumeBool(),
                [&](TRuntimeNode item) { return Pb_.Member(item, "Key"); },
                [&](TRuntimeNode item) { return Pb_.Member(item, "Payload"); },
                Fdp_.ConsumeBool(),
                count)
            : Pb_.ToSortedDict(rows, Fdp_.ConsumeBool(),
                [&](TRuntimeNode item) { return Pb_.Member(item, "Key"); },
                [&](TRuntimeNode item) { return Pb_.Member(item, "Payload"); },
                Fdp_.ConsumeBool(),
                count);

        TVector<TRuntimeNode> scores = {
            LengthScore(directDict),
            BoolScore(Pb_.HasItems(directDict)),
            LengthScore(contains),
            LengthScore(lookup),
            LengthScore(materializedDict),
            LengthScore(Pb_.DictKeys(materializedDict)),
            LengthScore(Pb_.DictPayloads(materializedDict)),
            LengthScore(Pb_.DictItems(materializedDict)),
        };
        return AddAll(scores);
    }

    TRuntimeNode BuildOptionalStructModel() {
        const auto optionalType = Pb_.NewOptionalType(I64Type_);
        const auto optional = Fdp_.ConsumeBool()
            ? Pb_.NewOptional(I64(-2048, 2048))
            : Pb_.NewEmptyOptional(optionalType);
        const auto fallback = I64(-2048, 2048);

        auto structValue = Pb_.NewEmptyStruct();
        structValue = Pb_.AddMember(structValue, "Number", Pb_.Coalesce(optional, fallback));
        structValue = Pb_.AddMember(structValue, "Text", SmallStringLiteral());
        structValue = Pb_.AddMember(structValue, "Exists", Pb_.Exists(optional));

        const auto removed = Pb_.RemoveMember(structValue, "Text", true);
        const auto list = Pb_.ToList(optional);
        const auto asOptional = Pb_.ToOptional(list);
        const auto defaulted = Pb_.Coalesce(asOptional, fallback);

        TVector<TRuntimeNode> scores = {
            Pb_.Member(structValue, "Number"),
            SizeScore(Pb_.Member(structValue, "Text")),
            BoolScore(Pb_.Member(structValue, "Exists")),
            Pb_.Member(removed, "Number"),
            BoolScore(Pb_.Member(removed, "Exists")),
            LengthScore(list),
            BoolScore(Pb_.Exists(Pb_.Head(list))),
            BoolScore(Pb_.Exists(Pb_.Last(list))),
            defaulted,
            Pb_.If(Pb_.Exists(optional), defaulted, fallback),
        };
        return AddAll(scores);
    }

    TRuntimeNode BuildJsonLikeModel() {
        static constexpr std::array<std::string_view, 8> JsonSamples = {{
            "null",
            "true",
            "false",
            "0",
            R"({"a":1,"b":[true,false,null]})",
            R"(["x",2,{"z":null}])",
            R"({"nested":{"n":-3},"s":"text"})",
            R"([])",
        }};
        static constexpr std::array<std::string_view, 6> YsonSamples = {{
            "#",
            "%true",
            "0",
            "{a=1;b=[%true;#;\"x\"]}",
            "[1;2;3]",
            "{nested={n=-3};s=\"text\"}",
        }};

        const auto jsonSample = JsonSamples[Fdp_.ConsumeIntegralInRange<size_t>(0, JsonSamples.size() - 1)];
        const auto ysonSample = YsonSamples[Fdp_.ConsumeIntegralInRange<size_t>(0, YsonSamples.size() - 1)];
        const auto jsonText = TString(jsonSample.data(), jsonSample.size());
        const auto ysonText = TString(ysonSample.data(), ysonSample.size());
        const auto rawText = SmallStringLiteral();
        const auto jsonString = StringLiteral(jsonText);
        const auto ysonString = StringLiteral(ysonText);
        const auto jsonLiteral = Pb_.NewDataLiteral<NUdf::EDataSlot::Json>(NUdf::TStringRef(jsonText.data(), jsonText.size()));
        const auto ysonLiteral = Pb_.NewDataLiteral<NUdf::EDataSlot::Yson>(NUdf::TStringRef(ysonText.data(), ysonText.size()));
        const auto parsedJson = Pb_.FromString(Fdp_.ConsumeBool() ? jsonString : rawText, JsonType_);
        const auto parsedYson = Pb_.FromString(Fdp_.ConsumeBool() ? ysonString : rawText, YsonType_);

        TVector<TRuntimeNode> scores = {
            SizeScore(Pb_.ToString(jsonLiteral)),
            SizeScore(Pb_.ToString(ysonLiteral)),
            BoolScore(Pb_.Exists(parsedJson)),
            BoolScore(Pb_.Exists(parsedYson)),
            SizeScore(Pb_.ToString(Pb_.Coalesce(parsedJson, jsonLiteral))),
            SizeScore(Pb_.ToString(Pb_.Coalesce(parsedYson, ysonLiteral))),
        };
        return AddAll(scores);
    }

private:
    TProgramBuilder& Pb_;
    FuzzedDataProvider& Fdp_;
    TType* const I64Type_;
    TType* const StringType_;
    TType* const JsonType_;
    TType* const YsonType_;
};

void RunOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    TMiniKqlRuntime runtime;
    TModelBuilder model(runtime.Builder(), fdp);
    runtime.Execute(model.BuildProgram());
}

} // namespace
} // namespace NKikimr::NMiniKQL

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    try {
        NKikimr::NMiniKQL::RunOneInput(data, size);
    } catch (const yexception&) {
    }
    return 0;
}

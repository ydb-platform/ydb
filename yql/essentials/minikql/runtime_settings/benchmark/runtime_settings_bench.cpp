#include <yql/essentials/minikql/runtime_settings/runtime_settings_hash.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings_serialization.h>

#include <benchmark/benchmark.h>

#include <util/string/cast.h>

namespace {

enum class ESettings {
    Empty,
    TString,
    TMap,
    Bool,
    ModuleSet,
    All,
    Udf,
};

void FillUdfSettings(NYql::TRuntimeSettings& settings) {
    for (const auto module : {"ModuleA", "ModuleB"}) {
        for (size_t index = 0; index < 8; ++index) {
            settings.SetUdfSetting(module, ToString(index), TString(64, 'a' + index));
        }
    }
}

void FillUdfSettings(NYql::TRuntimeSettings& settings, size_t count) {
    for (size_t index = 0; index < count; ++index) {
        settings.SetUdfSetting("Module", ToString(index), TString(64, 'a' + index % 26));
    }
}

void FillHostSettings(NYql::TRuntimeSettings& settings) {
    settings.DatumValidation.Set(NYql::EDatumValidationMode::Expensive);
    settings.UdfProfileEnable.Set(true);
    settings.UdfProfileMinTimeUs.Set(TDuration::MicroSeconds(12345));
    settings.UdfProfileGraceCount.Set(42);
    settings.UdfProfileHLLPrecision.Set(16);
}

NYql::TRuntimeSettings MakeSettings(ESettings scenario, size_t udfSettingCount = 0) {
    NYql::TRuntimeSettings settings;
    if (scenario == ESettings::TString) {
        settings.SetUdfSetting("Module", "Setting", TString(64, 'x'));
    }
    if (scenario == ESettings::TMap || scenario == ESettings::All) {
        FillUdfSettings(settings);
    }
    if (scenario == ESettings::Udf) {
        FillUdfSettings(settings, udfSettingCount);
    }
    if (scenario == ESettings::Bool || scenario == ESettings::All) {
        settings.TestHostSetting.Set(true);
    }
    if (scenario == ESettings::ModuleSet || scenario == ESettings::All) {
        settings.UdfProfileExcludeModules.Set({"ModuleC", "ModuleA", "ModuleB"});
    }
    if (scenario == ESettings::All) {
        FillHostSettings(settings);
    }
    return settings;
}

void Serialize(benchmark::State& state, ESettings scenario, size_t udfSettingCount = 0) {
    const auto settings = MakeSettings(scenario, udfSettingCount);
    for ([[maybe_unused]] auto iteration : state) {
        auto proto = NYql::SerializeRuntimeSettingsToProto(settings);
        benchmark::DoNotOptimize(proto);
    }
}

void Deserialize(benchmark::State& state, ESettings scenario, size_t udfSettingCount = 0) {
    const auto proto = NYql::SerializeRuntimeSettingsToProto(MakeSettings(scenario, udfSettingCount));
    for ([[maybe_unused]] auto iteration : state) {
        auto settings = NYql::DeserializeRuntimeSettingsFromProto(proto);
        benchmark::DoNotOptimize(settings);
    }
}

void Hash(benchmark::State& state, ESettings scenario, size_t udfSettingCount = 0) {
    const auto settings = MakeSettings(scenario, udfSettingCount);
    for ([[maybe_unused]] auto iteration : state) {
        auto hash = NYql::StableHashRuntimeSettings(settings);
        benchmark::DoNotOptimize(hash);
    }
}

void FillUdf(benchmark::State& state) {
    const auto input = MakeSettings(ESettings::Udf, state.range(0));
    for ([[maybe_unused]] auto iteration : state) {
        NYql::TRuntimeSettings settings;
        for (const auto& [module, values] : input.GetUdfSettings()) {
            for (const auto& [name, value] : values) {
                settings.SetUdfSetting(module, name, value);
            }
        }
        benchmark::DoNotOptimize(settings);
    }
}

void GetUdf(benchmark::State& state) {
    const auto settings = MakeSettings(ESettings::Udf, state.range(0));
    const auto name = ToString(state.range(0) / 2);
    for ([[maybe_unused]] auto iteration : state) {
        auto value = settings.GetUdfSetting("Module", name);
        benchmark::DoNotOptimize(value);
    }
}

void DefaultConstruct(benchmark::State& state) {
    for ([[maybe_unused]] auto iteration : state) {
        NYql::TRuntimeSettings settings;
        benchmark::DoNotOptimize(settings);
    }
}

template <typename TValue>
void Get(benchmark::State& state, const TValue& value) {
    NYql::TRuntimeSetting<TValue> setting{TValue{}};
    setting.Set(value);
    for ([[maybe_unused]] auto iteration : state) {
        const auto& value = setting.Get();
        auto* result = &value;
        benchmark::DoNotOptimize(result);
        benchmark::ClobberMemory();
    }
}

void GetTString(benchmark::State& state) {
    Get(state, TString(64, 'x'));
}

void GetTMap(benchmark::State& state) {
    const auto settings = MakeSettings(ESettings::TMap);
    Get(state, settings.GetUdfSettings());
}

void GetBool(benchmark::State& state) {
    Get(state, true);
}

void GetModuleSet(benchmark::State& state) {
    const auto settings = MakeSettings(ESettings::ModuleSet);
    Get(state, settings.UdfProfileExcludeModules.Get());
}

BENCHMARK_CAPTURE(Serialize, Empty, ESettings::Empty);
BENCHMARK_CAPTURE(Deserialize, Empty, ESettings::Empty);
BENCHMARK_CAPTURE(Hash, Empty, ESettings::Empty);

BENCHMARK_CAPTURE(Serialize, TString, ESettings::TString);
BENCHMARK_CAPTURE(Deserialize, TString, ESettings::TString);
BENCHMARK_CAPTURE(Hash, TString, ESettings::TString);

BENCHMARK_CAPTURE(Serialize, TMap, ESettings::TMap);
BENCHMARK_CAPTURE(Deserialize, TMap, ESettings::TMap);
BENCHMARK_CAPTURE(Hash, TMap, ESettings::TMap);

BENCHMARK_CAPTURE(Serialize, Bool, ESettings::Bool);
BENCHMARK_CAPTURE(Deserialize, Bool, ESettings::Bool);
BENCHMARK_CAPTURE(Hash, Bool, ESettings::Bool);

BENCHMARK_CAPTURE(Serialize, ModuleSet, ESettings::ModuleSet);
BENCHMARK_CAPTURE(Deserialize, ModuleSet, ESettings::ModuleSet);
BENCHMARK_CAPTURE(Hash, ModuleSet, ESettings::ModuleSet);

BENCHMARK_CAPTURE(Serialize, All, ESettings::All);
BENCHMARK_CAPTURE(Deserialize, All, ESettings::All);
BENCHMARK_CAPTURE(Hash, All, ESettings::All);

BENCHMARK_CAPTURE(Serialize, Udf1, ESettings::Udf, 1);
BENCHMARK_CAPTURE(Deserialize, Udf1, ESettings::Udf, 1);
BENCHMARK_CAPTURE(Hash, Udf1, ESettings::Udf, 1);

BENCHMARK_CAPTURE(Serialize, Udf5, ESettings::Udf, 5);
BENCHMARK_CAPTURE(Deserialize, Udf5, ESettings::Udf, 5);
BENCHMARK_CAPTURE(Hash, Udf5, ESettings::Udf, 5);

BENCHMARK_CAPTURE(Serialize, Udf10, ESettings::Udf, 10);
BENCHMARK_CAPTURE(Deserialize, Udf10, ESettings::Udf, 10);
BENCHMARK_CAPTURE(Hash, Udf10, ESettings::Udf, 10);

BENCHMARK_CAPTURE(Serialize, Udf50, ESettings::Udf, 50);
BENCHMARK_CAPTURE(Deserialize, Udf50, ESettings::Udf, 50);
BENCHMARK_CAPTURE(Hash, Udf50, ESettings::Udf, 50);

BENCHMARK(FillUdf)->Arg(1)->Arg(5)->Arg(10)->Arg(50);
BENCHMARK(GetUdf)->Arg(1)->Arg(5)->Arg(10)->Arg(50);

BENCHMARK(DefaultConstruct);
BENCHMARK(GetTString);
BENCHMARK(GetTMap);
BENCHMARK(GetBool);
BENCHMARK(GetModuleSet);

} // namespace

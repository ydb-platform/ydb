#include <library/cpp/lua/json.h>
#include <library/cpp/lua/wrapper.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

namespace {

constexpr size_t MaxInputSize = 64 * 1024;
constexpr ui16 MaxGeneratedDepth = 128;

NJson::TJsonValue MakeNestedJsonValue(FuzzedDataProvider& provider) {
    const ui16 depth = provider.ConsumeIntegralInRange<ui16>(0, MaxGeneratedDepth);
    const bool objectShape = provider.ConsumeBool();

    NJson::TJsonValue value(0);
    for (ui16 i = 0; i < depth; ++i) {
        if (objectShape) {
            NJson::TJsonValue parent(NJson::JSON_MAP);
            parent.InsertValue("k", std::move(value));
            value = std::move(parent);
        } else {
            NJson::TJsonValue parent(NJson::JSON_ARRAY);
            parent.AppendValue(std::move(value));
            value = std::move(parent);
        }
    }
    return value;
}

void Exercise(const NJson::TJsonValue& value) {
    try {
        TLuaStateHolder state;
        NLua::PushJsonValue(&state, value);
        state.pop(state.on_stack());
    } catch (...) {
    }
}

void ExerciseRaw(TStringBuf raw) {
    if (raw.size() > MaxInputSize) {
        return;
    }

    try {
        NJson::TJsonValue value;
        if (NJson::ReadJsonTree(raw, &value, false)) {
            Exercise(value);
        }
    } catch (...) {
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    ExerciseRaw(TStringBuf(reinterpret_cast<const char*>(data), size));

    FuzzedDataProvider provider(data, size);
    Exercise(MakeNestedJsonValue(provider));

    return 0;
}

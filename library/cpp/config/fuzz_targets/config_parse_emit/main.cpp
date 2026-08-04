#include <library/cpp/config/config.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/stream/str.h>
#include <util/string/cast.h>
#include <util/system/yassert.h>

namespace {

enum class EConfigMode {
    Json,
    Ini,
    Markup,
};

TString ConsumeToken(FuzzedDataProvider& fdp, size_t maxLen) {
    static const TStringBuf Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
    const int len = fdp.ConsumeIntegralInRange<int>(0, static_cast<int>(maxLen));
    TString out;
    out.reserve(len);
    for (int i = 0; i < len; ++i) {
        out.push_back(Alphabet[fdp.ConsumeIntegralInRange<int>(0, static_cast<int>(Alphabet.size() - 1))]);
    }
    return out;
}

TString ScrubPreprocessorExpressions(TString input) {
    size_t pos = 0;
    while ((pos = input.find("${", pos)) != TString::npos) {
        input[pos + 1] = ' ';
        pos += 2;
    }
    return input;
}

TString ConsumeTextInput(FuzzedDataProvider& fdp, size_t maxLen) {
    TString input = fdp.ConsumeRandomLengthString(maxLen);
    for (char& ch : input) {
        const unsigned char value = static_cast<unsigned char>(ch);
        if (value == '\n' || value == '\r' || value == '\t') {
            continue;
        }
        if (value < 0x20 || value >= 0x7f) {
            ch = ' ';
        }
    }
    return ScrubPreprocessorExpressions(input);
}

bool HasPreprocessorExpression(TStringBuf input) {
    return input.find("${") != TStringBuf::npos;
}

TString JsonString(const TString& value) {
    TString out = "\"";
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    out += '"';
    return out;
}

TString BuildJsonValue(FuzzedDataProvider& fdp, int depth) {
    const int choice = depth >= 3 ? fdp.ConsumeIntegralInRange<int>(0, 4) : fdp.ConsumeIntegralInRange<int>(0, 6);
    switch (choice) {
        case 0:
            return "null";
        case 1:
            return fdp.ConsumeBool() ? "true" : "false";
        case 2:
            return ToString(fdp.ConsumeIntegralInRange<i64>(-1000000, 1000000));
        case 3:
            return ToString(fdp.ConsumeFloatingPointInRange<double>(-1000000.0, 1000000.0));
        case 4:
            return JsonString(ConsumeToken(fdp, 24));
        case 5: {
            TString out = "[";
            const int size = fdp.ConsumeIntegralInRange<int>(0, 4);
            for (int i = 0; i < size; ++i) {
                if (i) {
                    out += ',';
                }
                out += BuildJsonValue(fdp, depth + 1);
            }
            out += ']';
            return out;
        }
        default: {
            TString out = "{";
            const int size = fdp.ConsumeIntegralInRange<int>(0, 4);
            for (int i = 0; i < size; ++i) {
                if (i) {
                    out += ',';
                }
                out += JsonString(ConsumeToken(fdp, 12));
                out += ':';
                out += BuildJsonValue(fdp, depth + 1);
            }
            out += '}';
            return out;
        }
    }
}

TString BuildIni(FuzzedDataProvider& fdp) {
    TString out;
    const int sections = fdp.ConsumeIntegralInRange<int>(0, 3);
    for (int section = 0; section < sections; ++section) {
        out += '[';
        out += ConsumeToken(fdp, 10);
        if (fdp.ConsumeBool()) {
            out += '.';
            out += ConsumeToken(fdp, 10);
        }
        out += "]\n";
        const int keys = fdp.ConsumeIntegralInRange<int>(0, 4);
        for (int i = 0; i < keys; ++i) {
            out += ConsumeToken(fdp, 12);
            out += '=';
            out += ConsumeToken(fdp, 32);
            out += '\n';
        }
    }
    return out;
}

TString BuildMarkupNode(FuzzedDataProvider& fdp, int depth) {
    TString tag = ConsumeToken(fdp, 8);
    if (tag.empty()) {
        tag = "node";
    }
    TString out = "<";
    out += tag;
    const int attrs = fdp.ConsumeIntegralInRange<int>(0, 3);
    for (int i = 0; i < attrs; ++i) {
        out += ' ';
        out += ConsumeToken(fdp, 8);
        out += "=\"";
        out += ConsumeToken(fdp, 16);
        out += '"';
    }
    out += '>';
    if (depth < 2 && fdp.ConsumeBool()) {
        const int children = fdp.ConsumeIntegralInRange<int>(0, 3);
        for (int i = 0; i < children; ++i) {
            out += BuildMarkupNode(fdp, depth + 1);
        }
    } else {
        const int keys = fdp.ConsumeIntegralInRange<int>(0, 4);
        for (int i = 0; i < keys; ++i) {
            out += ConsumeToken(fdp, 8);
            out += ':';
            out += ConsumeToken(fdp, 16);
            out += '\n';
        }
    }
    out += "</";
    out += tag;
    out += '>';
    return out;
}

TString BuildInput(FuzzedDataProvider& fdp, EConfigMode mode) {
    if (!fdp.ConsumeBool()) {
        return ConsumeTextInput(fdp, 2048);
    }

    switch (mode) {
        case EConfigMode::Json:
            return BuildJsonValue(fdp, 0);
        case EConfigMode::Ini:
            return BuildIni(fdp);
        case EConfigMode::Markup:
            return BuildMarkupNode(fdp, 0);
    }
    Y_ABORT_UNLESS(false);
    return {};
}

TString ToJson(const NConfig::TConfig& config) {
    TString out;
    TStringOutput stream(out);
    config.ToJson(stream);
    return out;
}

TString DumpJson(const NConfig::TConfig& config) {
    TString out;
    TStringOutput stream(out);
    config.DumpJson(stream);
    return out;
}

void TouchConfig(const NConfig::TConfig& config, int depth = 0) {
    if (depth >= 4 || config.IsNull()) {
        return;
    }

    if (config.IsA<NConfig::TDict>()) {
        int visited = 0;
        for (const auto& [key, value] : config.Get<NConfig::TDict>()) {
            Y_ABORT_UNLESS(config.Has(key) == !value.IsNull());
            Y_ABORT_UNLESS(config[key].IsNull() == value.IsNull());
            Y_ABORT_UNLESS(config.At(key).IsNull() == value.IsNull());
            (void)config.At(key);
            TouchConfig(value, depth + 1);
            if (++visited == 8) {
                break;
            }
        }
        return;
    }

    if (config.IsA<NConfig::TArray>()) {
        const size_t size = config.GetArraySize();
        for (size_t i = 0; i < size && i < 8; ++i) {
            (void)config[i];
            (void)config.Get<NConfig::TArray>().At(i);
            TouchConfig(config[i], depth + 1);
        }
        return;
    }

    try {
        (void)config.As<TString>();
    } catch (...) {
    }
    try {
        (void)config.As<bool>();
    } catch (...) {
    }
    try {
        (void)config.As<i64>();
    } catch (...) {
    }
    try {
        (void)config.As<ui64>();
    } catch (...) {
    }
    try {
        (void)config.As<double>();
    } catch (...) {
    }
}

void CheckJsonEmitStable(const NConfig::TConfig& config) {
    TouchConfig(config);

    const TString compactJson = ToJson(config);
    if (HasPreprocessorExpression(compactJson)) {
        return;
    }

    const NConfig::TConfig reparsed = NConfig::TConfig::ReadJson(compactJson);
    Y_ABORT_UNLESS(ToJson(reparsed) == compactJson);

    const TString prettyJson = DumpJson(config);
    if (!HasPreprocessorExpression(prettyJson)) {
        const NConfig::TConfig prettyReparsed = NConfig::TConfig::ReadJson(prettyJson);
        Y_ABORT_UNLESS(ToJson(prettyReparsed) == compactJson);

        TString saved;
        TStringOutput output(saved);
        config.Save(&output);

        TStringInput input(saved);
        NConfig::TConfig loaded;
        loaded.Load(&input);
        Y_ABORT_UNLESS(ToJson(loaded) == compactJson);
    }
}

NConfig::TConfig ParseConfig(TStringBuf input, EConfigMode mode) {
    switch (mode) {
        case EConfigMode::Json:
            return NConfig::TConfig::ReadJson(input);
        case EConfigMode::Ini:
            return NConfig::TConfig::ReadIni(input);
        case EConfigMode::Markup:
            return NConfig::TConfig::ReadMarkup(input);
    }
    Y_ABORT_UNLESS(false);
    return {};
}

void FuzzConfigParseEmit(FuzzedDataProvider& fdp) {
    const auto mode = static_cast<EConfigMode>(fdp.ConsumeIntegralInRange<int>(0, 2));
    const TString input = BuildInput(fdp, mode);

    NConfig::TConfig config;
    try {
        config = ParseConfig(input, mode);
    } catch (...) {
        return;
    }

    CheckJsonEmitStable(config);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    FuzzConfigParseEmit(fdp);

    return 0;
}

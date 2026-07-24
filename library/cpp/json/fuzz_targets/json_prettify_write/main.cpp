#include <library/cpp/json/json_prettifier.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/stream/mem.h>
#include <util/stream/str.h>

namespace {

void ConfigureReader(NJson::TJsonReaderConfig& config, FuzzedDataProvider& fdp) {
    config.UseIterativeParser = fdp.ConsumeBool();
    config.AllowComments = fdp.ConsumeBool();
    config.DontValidateUtf8 = fdp.ConsumeBool();
    config.AllowEscapedApostrophe = fdp.ConsumeBool();
    config.AllowReadNanInf = fdp.ConsumeBool();
    config.MaxDepth = fdp.ConsumeIntegralInRange<ui64>(0, 256);
    config.SetBufferSize(fdp.ConsumeIntegralInRange<size_t>(1, 4096));
}

void ConfigurePrettifier(NJson::TJsonPrettifier& prettifier, FuzzedDataProvider& fdp) {
    prettifier.Unquote = fdp.ConsumeBool();
    prettifier.Padding = fdp.ConsumeIntegralInRange<ui8>(0, 8);
    prettifier.SingleQuotes = fdp.ConsumeBool();
    prettifier.Compactify = fdp.ConsumeBool();
    prettifier.Strict = fdp.ConsumeBool();
    prettifier.NewUnquote = fdp.ConsumeBool();
    prettifier.MaxPaddingLevel = fdp.ConsumeIntegralInRange<ui32>(0, 32);
}

void ConfigureWriter(NJson::TJsonWriterConfig& config, FuzzedDataProvider& fdp) {
    config.SetUnbuffered(fdp.ConsumeBool());
    config.SetValidateUtf8(fdp.ConsumeBool());
    config.SetFormatOutput(fdp.ConsumeBool());
    config.SortKeys = fdp.ConsumeBool();
    config.DontEscapeStrings = fdp.ConsumeBool();
    config.WriteNanAsString = fdp.ConsumeBool();
}

void FuzzJson(FuzzedDataProvider& fdp) {
    NJson::TJsonReaderConfig readerConfig;
    ConfigureReader(readerConfig, fdp);

    NJson::TJsonPrettifier prettifier;
    ConfigurePrettifier(prettifier, fdp);

    NJson::TJsonWriterConfig writerConfig;
    ConfigureWriter(writerConfig, fdp);

    const TString input = fdp.ConsumeRemainingBytesAsString();

    (void)NJson::TJsonPrettifier::MayUnquoteNew(input);
    (void)NJson::TJsonPrettifier::MayUnquoteOld(input);
    (void)prettifier.Prettify(input);
    (void)NJson::PrettifyJson(input, prettifier.Unquote, prettifier.Padding, prettifier.SingleQuotes);
    (void)NJson::CompactifyJson(input, prettifier.Unquote, prettifier.SingleQuotes);

    NJson::TJsonValue tree;
    const bool readTreeOk = NJson::ReadJsonTree(input, &readerConfig, &tree, false);
    (void)NJson::ValidateJson(input, readerConfig, false);

    {
        TMemoryInput inputStream(input.data(), input.size());
        NJson::TJsonCallbacks callbacks;
        (void)NJson::ReadJson(&inputStream, &readerConfig, &callbacks);
    }

    {
        TMemoryInput inputStream(input.data(), input.size());
        NJson::TJsonValue parsedViaCallbacks;
        NJson::TParserCallbacks callbacks(parsedViaCallbacks, false, fdp.ConsumeBool());
        (void)NJson::ReadJson(&inputStream, &readerConfig, &callbacks);
    }

    if (readTreeOk) {
        TStringStream output;
        NJson::WriteJson(&output, &tree, writerConfig);
        const TString serialized = output.Str();

        TStringStream manualOutput;
        NJson::TJsonWriter writer(&manualOutput, writerConfig);
        writer.Write(tree);
        writer.Flush();

        NJson::TJsonValue reparsed;
        (void)NJson::ReadJsonTree(serialized, &readerConfig, &reparsed, false);
        (void)prettifier.Prettify(serialized);
        (void)NJson::CompactifyJson(serialized, prettifier.Unquote, prettifier.SingleQuotes);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        FuzzedDataProvider fdp(data, size);
        FuzzJson(fdp);
    } catch (...) {
    }

    return 0;
}

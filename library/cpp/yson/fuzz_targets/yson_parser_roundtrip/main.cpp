#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/parser.h>
#include <library/cpp/yson/tokenizer.h>
#include <library/cpp/yson/writer.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/maybe.h>
#include <util/stream/mem.h>
#include <util/stream/str.h>

namespace {

void Tokenize(TStringBuf input) {
    NYson::TTokenizer tokenizer(input);
    while (tokenizer.ParseNext()) {
        (void)tokenizer.CurrentToken();
        (void)tokenizer.GetCurrentType();
        (void)tokenizer.GetCurrentSuffix();
        (void)tokenizer.CurrentInput();
    }
}

void ParseAndRoundTrip(TStringBuf input, ::NYson::EYsonType type, bool enableLinePositions, TMaybe<ui64> memoryLimit) {
    {
        NYT::TNode node;
        NYT::TNodeBuilder builder(&node);
        NYson::TStatelessYsonParser parser(&builder, enableLinePositions, memoryLimit);
        parser.Parse(input, type);

        const TString text = NYT::NodeToYsonString(node, ::NYson::EYsonFormat::Text);
        const TString binary = NYT::NodeToYsonString(node, ::NYson::EYsonFormat::Binary);
        const TString pretty = NYT::NodeToYsonString(node, ::NYson::EYsonFormat::Pretty);
        (void)NYT::NodeToCanonicalYsonString(node, ::NYson::EYsonFormat::Text);

        TString withTail = binary;
        withTail.append("tail");
        TMemoryInput binaryInput(withTail.data(), withTail.size());
        auto reparsed = NYT::NodeFromYsonStreamNonGreedy(&binaryInput, type);
        (void)NYT::NodeToCanonicalYsonString(reparsed, ::NYson::EYsonFormat::Text);

        {
            TMemoryInput inputStream(text.data(), text.size());
            TString reformatted;
            TStringOutput output(reformatted);
            NYson::ReformatYsonStream(&inputStream, &output, ::NYson::EYsonFormat::Pretty, type);
        }

        {
            TMemoryInput inputStream(pretty.data(), pretty.size());
            NYT::TNode streamNode;
            NYT::TNodeBuilder streamBuilder(&streamNode);
            NYson::TYsonParser parser(&streamBuilder, &inputStream, type, enableLinePositions, 4096, true, memoryLimit);
            parser.Parse();
            (void)NYT::NodeToCanonicalYsonString(streamNode, ::NYson::EYsonFormat::Text);
        }
    }

    {
        NYT::TNode node;
        NYT::TNodeBuilder builder(&node);
        NYson::ParseYsonStringBuffer(input, &builder, type, enableLinePositions, memoryLimit);
        (void)NYT::NodeToYsonString(node, ::NYson::EYsonFormat::Text);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    const bool enableLinePositions = fdp.ConsumeBool();
    const TMaybe<ui64> memoryLimit = fdp.ConsumeBool()
        ? TMaybe<ui64>(fdp.ConsumeIntegralInRange<ui64>(1, 4096))
        : Nothing();
    const TString input = fdp.ConsumeRemainingBytesAsString();
    const TStringBuf payload(input);

    try {
        Tokenize(payload);
    } catch (...) {
    }

    for (auto type : {::NYson::EYsonType::Node, ::NYson::EYsonType::ListFragment}) {
        try {
            ParseAndRoundTrip(payload, type, enableLinePositions, memoryLimit);
        } catch (...) {
        }
    }

    return 0;
}

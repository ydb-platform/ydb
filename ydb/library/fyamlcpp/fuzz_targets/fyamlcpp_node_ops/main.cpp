#include <ydb/library/fyamlcpp/fyamlcpp.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace {

TString ConsumeAscii(FuzzedDataProvider& fdp, size_t maxLen = 16) {
    TString value = fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, maxLen));
    for (char& ch : value) {
        if (!(('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z') || ('0' <= ch && ch <= '9'))) {
            ch = '_';
        }
    }
    return value;
}

TString MakeScalarPayload(FuzzedDataProvider& fdp) {
    TString value = fdp.ConsumeBytesAsString(fdp.ConsumeIntegralInRange<size_t>(0, 32));
    if (value.empty()) {
        value = "value";
    }
    return value;
}

NKikimr::NFyaml::TDocument ParseOrFallback(const TString& input) {
    try {
        return NKikimr::NFyaml::TDocument::Parse(input);
    } catch (...) {
        return NKikimr::NFyaml::TDocument::Parse("{}");
    }
}

void InspectNodeImpl(const NKikimr::NFyaml::TNodeRef& node, size_t depth, size_t& budget) {
    if (depth >= 128 || budget == 0) {
        return;
    }
    --budget;

    (void)node.Path();
    (void)node.Type();
    (void)node.Tag();
    (void)node.Style();
    (void)node.BeginMark();
    (void)node.EndMark();
    (void)node.EmitToCharArray();

    if (node.IsAlias()) {
        (void)node.ResolveAlias();
    }

    switch (node.Type()) {
        case NKikimr::NFyaml::ENodeType::Scalar:
            (void)node.Scalar();
            break;
        case NKikimr::NFyaml::ENodeType::Mapping: {
            auto map = node.Map();
            (void)map.size();
            (void)map.empty();
            for (auto it = map.begin(); it != map.end(); ++it) {
                (void)it->Key().Scalar();
                InspectNodeImpl(it->Value(), depth + 1, budget);
            }
            for (auto it = map.rbegin(); it != map.rend(); ++it) {
                (void)it->Key().Scalar();
            }
            break;
        }
        case NKikimr::NFyaml::ENodeType::Sequence: {
            auto seq = node.Sequence();
            (void)seq.size();
            (void)seq.empty();
            for (auto it = seq.begin(); it != seq.end(); ++it) {
                InspectNodeImpl(*it, depth + 1, budget);
            }
            for (auto it = seq.rbegin(); it != seq.rend(); ++it) {
                (void)it->Type();
            }
            break;
        }
    }
}

void InspectNode(const NKikimr::NFyaml::TNodeRef& node) {
    size_t budget = 4096;
    InspectNodeImpl(node, 0, budget);
}

void MutateDocument(NKikimr::NFyaml::TDocument& doc, FuzzedDataProvider& fdp) {
    auto root = doc.Root();
    const TString tag = TStringBuilder() << "!fuzz_" << ConsumeAscii(fdp);

    try {
        root.SetTag(tag);
        (void)root.Tag();
        root.RemoveTag();
    } catch (...) {
    }

    switch (root.Type()) {
        case NKikimr::NFyaml::ENodeType::Scalar:
            try {
                root.SetStyle(fdp.ConsumeBool()
                    ? NKikimr::NFyaml::ENodeStyle::Plain
                    : NKikimr::NFyaml::ENodeStyle::DoubleQuoted);
            } catch (...) {
            }
            break;
        case NKikimr::NFyaml::ENodeType::Mapping: {
            auto map = root.Map();
            auto key = doc.CreateScalar(TStringBuilder() << "key_" << ConsumeAscii(fdp));
            auto value = doc.CreateScalar(MakeScalarPayload(fdp));
            map.Append(key, value);
            (void)map.Has(key.Scalar());
            if (auto pair = map.pair_at_opt(key.Scalar())) {
                auto copied = pair.Value().Copy(doc);
                pair.SetValue(copied.Ref());
                if (fdp.ConsumeBool()) {
                    map.Remove(pair);
                }
            }
            break;
        }
        case NKikimr::NFyaml::ENodeType::Sequence: {
            auto seq = root.Sequence();
            auto value = doc.CreateScalar(MakeScalarPayload(fdp));
            if (fdp.ConsumeBool()) {
                seq.Prepend(value);
            } else {
                seq.Append(value);
            }
            if (!seq.empty()) {
                auto first = seq.at(0);
                (void)first.Copy(doc);
                if (fdp.ConsumeBool()) {
                    seq.Remove(first);
                }
            }
            break;
        }
    }

    (void)doc.EmitToCharArray();
}

void FuzzYamlNodeOps(const uint8_t* data, size_t size) {
    if (size > 128 * 1024) {
        return;
    }

    FuzzedDataProvider fdp(data, size);
    const TString input = fdp.ConsumeRemainingBytesAsString();

    try {
        auto parser = NKikimr::NFyaml::TParser::Create(input);
        size_t parsedDocs = 0;
        while (parsedDocs < 8) {
            auto doc = parser.NextDocument();
            if (!doc) {
                break;
            }
            InspectNode(doc->Root());
            auto clone = doc->Clone();
            MutateDocument(clone, fdp);
            ++parsedDocs;
        }
    } catch (...) {
    }

    auto doc = ParseOrFallback(input);
    InspectNode(doc.Root());

    auto clone = doc.Clone();
    MutateDocument(clone, fdp);

    try {
        clone.Resolve();
    } catch (...) {
    }

    try {
        (void)NKikimr::NFyaml::TJsonEmitter(clone.Root()).EmitToCharArray();
    } catch (...) {
    }

    auto copiedRoot = doc.Root().Copy(clone);
    (void)copiedRoot.Ref().DeepEqual(clone.Root());
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        FuzzYamlNodeOps(data, size);
    } catch (...) {
    }

    return 0;
}

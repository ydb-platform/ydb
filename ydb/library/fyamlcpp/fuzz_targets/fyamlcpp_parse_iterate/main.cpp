// Fuzzer for TDocument::Parse + full document node iteration.
// TDocument::Parse is already fuzzed in fyamlcpp_parse, but this target
// additionally walks the parsed node tree (via TDocumentNodeIterator),
// resolves aliases, and emits back to string.
// Deep iteration exercises different code paths in the libfyaml wrapper
// that simple single-parse fuzzing misses, e.g. anchor/alias resolution
// loops and sequence/mapping traversal. Config submissions via admin
// gRPC ConsoleService go through this iteration path.
#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 128 * 1024) return 0;
    TString input(reinterpret_cast<const char*>(data), size);
    try {
        auto doc = NKikimr::NFyaml::TDocument::Parse(input);
        auto root = doc.Root();
        if (!root.Empty()) {
            // Iterate every node in the document
            for (auto it = doc.begin(); it != doc.end(); ++it) {
                auto node = *it;
                (void)node.Type();
                // Try to emit the node back to char array
                try {
                    auto arr = node.EmitToCharArray();
                    (void)arr;
                } catch (...) {}
            }
            // Also emit the full document
            try {
                auto dumped = doc.EmitToCharArray();
                (void)dumped;
            } catch (...) {}
        }
    } catch (...) {}
    return 0;
}

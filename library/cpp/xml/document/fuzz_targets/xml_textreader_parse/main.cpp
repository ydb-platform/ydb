#include <library/cpp/xml/document/xml-textreader.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        TStringInput stream(input);
        NXml::TTextReader reader(stream);
        while (reader.Read()) {
            auto nodeType = reader.GetNodeType();
            (void)nodeType;
            if (reader.HasValue()) {
                auto value = reader.GetValue();
                (void)value;
            }
            auto name = reader.GetName();
            (void)name;
            auto depth = reader.GetDepth();
            (void)depth;
        }
    } catch (...) {}
    return 0;
}

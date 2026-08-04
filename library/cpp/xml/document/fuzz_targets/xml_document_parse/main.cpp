#include <library/cpp/xml/document/xml-document.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        NXml::TDocument doc(input, NXml::TDocument::String);
        auto root = doc.Root();
        (void)root;
    } catch (...) {}
    return 0;
}

#include <ydb/core/http_proxy/sqs_xml/params.h>

#include <cstddef>
#include <cstdint>
#include <util/generic/string.h>

using namespace NKikimr::NHttpProxy::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size < 2) {
        return 0;
    }

    try {
        TParameters params;
        TParametersParser parser(&params);

        const size_t mid = size / 2;
        TString name(reinterpret_cast<const char*>(data), mid);
        TString value(reinterpret_cast<const char*>(data + mid), size - mid);
        if (!name.empty() && !value.empty()) {
            parser.Append(name, value);
        }
    } catch (...) {
    }

    return 0;
}

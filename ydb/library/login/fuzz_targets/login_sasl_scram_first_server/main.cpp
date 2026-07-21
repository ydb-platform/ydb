// Fuzzer for SCRAM first server message parser.
// Parses server-first-message that arrives from a potentially malicious
// or fuzzer-controlled server response during SCRAM negotiation.
#include <ydb/library/login/sasl/scram.h>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    std::string input(reinterpret_cast<const char*>(data), size);
    NLogin::NSasl::TFirstServerMsg parsed;
    try {
        NLogin::NSasl::ParseFirstServerMsg(input, parsed);
    } catch (...) {}
    return 0;
}

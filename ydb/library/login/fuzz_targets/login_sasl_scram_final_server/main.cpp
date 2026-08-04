// Fuzzer for SCRAM final server message parser.
// Parses server-final-message (contains server signature or error).
// Validates e= error fields and server signatures in SCRAM-SHA-256.
#include <ydb/library/login/sasl/scram.h>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    std::string input(reinterpret_cast<const char*>(data), size);
    NLogin::NSasl::TFinalServerMsg parsed;
    try {
        NLogin::NSasl::ParseFinalServerMsg(input, parsed);
    } catch (...) {}
    return 0;
}

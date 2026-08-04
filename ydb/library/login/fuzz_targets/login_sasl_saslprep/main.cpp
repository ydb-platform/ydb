// Fuzzer for the SASLprep Unicode normalization function.
// SASLprep is called on usernames and passwords during SASL authentication
// (SCRAM-SHA-256 / PLAIN), reachable over gRPC and PgWire.
// It performs Unicode string normalization (NFKC) plus prohibited-character
// checking, which involves complex character tables - a classic crash surface.
#include <ydb/library/login/sasl/saslprep.h>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    std::string input(reinterpret_cast<const char*>(data), size);
    std::string result;
    try {
        NLogin::NSasl::SaslPrep(input, result);
    } catch (...) {}
    return 0;
}

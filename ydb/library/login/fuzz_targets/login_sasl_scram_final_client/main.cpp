// Fuzzer for SCRAM final client message parser.
// ParseFinalClientMsg handles the client-final-message that carries the SCRAM proof.
// Reachable via gRPC/PgWire SCRAM-SHA-256 authentication handshake.
#include <ydb/library/login/sasl/scram.h>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    std::string input(reinterpret_cast<const char*>(data), size);
    NLogin::NSasl::TFinalClientMsg parsed;
    try {
        NLogin::NSasl::ParseFinalClientMsg(input, parsed);
    } catch (...) {}
    return 0;
}

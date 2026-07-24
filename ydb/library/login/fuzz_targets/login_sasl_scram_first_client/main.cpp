// Fuzzer for SCRAM first client message parser.
// SCRAM auth messages arrive over gRPC/PgWire authentication handshake.
// Malformed first-client-message can trigger parsing bugs in the auth path.
#include <ydb/library/login/sasl/scram.h>
#include <string>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    std::string input(reinterpret_cast<const char*>(data), size);
    NLogin::NSasl::TFirstClientMsg parsed;
    try {
        NLogin::NSasl::ParseFirstClientMsg(input, parsed);
    } catch (...) {}
    return 0;
}

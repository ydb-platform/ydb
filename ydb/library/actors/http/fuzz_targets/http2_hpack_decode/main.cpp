// Fuzzer for HTTP/2 HPACK header decompression.
// THPackDecoder::Decode processes the compressed header block from HTTP/2
// HEADERS and CONTINUATION frames. Every gRPC request over HTTP/2 carries
// a HPACK-encoded header block - this is a high-priority attack surface.
// Bugs here (OOB reads, integer overflows in integer decoding, Huffman
// table traversal) are reachable remotely without authentication.
#include <ydb/library/actors/http/http2_hpack.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 65536) return 0;

    TStringBuf input(reinterpret_cast<const char*>(data), size);
    NHttp::NHttp2::THPackDecoder decoder;
    TVector<std::pair<TString, TString>> headers;
    try {
        decoder.Decode(input, headers);
    } catch (...) {}
    return 0;
}

#include "decoder.h"

#include <library/cpp/tvmauth/client/misc/utils.h>

#include <library/cpp/openssl/crypto/sha.h>
#include <library/cpp/streams/brotli/brotli.h>
#include <library/cpp/streams/zstd/zstd.h>

#include <util/generic/yexception.h>
#include <util/stream/zlib.h>
#include <util/string/ascii.h>

namespace NTvmAuth::NRoles {
    TString TDecoder::Decode(const TStringBuf codec, TString&& blob) {
        if (codec.empty()) {
            return std::move(blob);
        }

        const TCodecInfo info = ParseCodec(codec);
        TString decoded = DecodeImpl(info.Type, blob);

        VerifySize(decoded, info.Size);
        VerifyChecksum(decoded, info.Sha256);

        return decoded;
    }

    TDecoder::TCodecInfo TDecoder::ParseCodec(TStringBuf codec) {
        const char delim = ':';

        const TStringBuf version = codec.NextTok(delim);
        Y_ENSURE(version == "1",
                 "unknown codec format version; known: 1; got: " << version);

        TCodecInfo res;
        res.Type = codec.NextTok(delim);
        Y_ENSURE(res.Type, "codec type is empty");

        const TStringBuf size = codec.NextTok(delim);
        Y_ENSURE(TryIntFromString<10>(size, res.Size),
                 "decoded blob size is not number");

        res.Sha256 = codec;
        const size_t expectedSha256Size = 2 * NOpenSsl::NSha256::DIGEST_LENGTH;
        Y_ENSURE(res.Sha256.size() == expectedSha256Size,
                 "sha256 of decoded blob has invalid length: expected "
                     << expectedSha256Size << ", got " << res.Sha256.size());

        return res;
    }

    TString TDecoder::DecodeImpl(TStringBuf codec, const TString& blob) {
        if (AsciiEqualsIgnoreCase(codec, "brotli")) {
            return DecodeBrolti(blob);
        } else if (AsciiEqualsIgnoreCase(codec, "gzip")) {
            return DecodeGzip(blob);
        } else if (AsciiEqualsIgnoreCase(codec, "zstd")) {
            return DecodeZstd(blob);
        }

        ythrow yexception() << "unknown codec: '" << codec << "'";
    }

    TString TDecoder::DecodeBrolti(const TString& blob) {
        TStringInput in(blob);
        return TBrotliDecompress(&in).ReadAll();
    }

    TString TDecoder::DecodeGzip(const TString& blob) {
        TStringInput in(blob);
        return TZLibDecompress(&in).ReadAll();
    }

    TString TDecoder::DecodeZstd(const TString& blob) {
        TStringInput in(blob);
        return TZstdDecompress(&in).ReadAll();
    }

    void TDecoder::VerifySize(const TStringBuf decoded, size_t expected) {
        Y_ENSURE(expected == decoded.size(),
                 "Decoded blob has bad size: expected " << expected << ", actual " << decoded.size());
    }

    void TDecoder::VerifyChecksum(const TStringBuf decoded, const TStringBuf expected) {
        using namespace NOpenSsl::NSha256;

        const TDigest dig = Calc(decoded);
        const TString actual = NUtils::ToHex(TStringBuf((char*)dig.data(), dig.size()));

        Y_ENSURE(AsciiEqualsIgnoreCase(actual, expected),
                 "Decoded blob has bad sha256: expected=" << expected << ", actual=" << actual);
    }
}

#include <library/cpp/tvmauth/client/exception.h> 
#include <library/cpp/tvmauth/client/misc/roles/decoder.h> 
 
#include <library/cpp/tvmauth/unittest.h> 
#include <library/cpp/tvmauth/src/utils.h> 
 
#include <library/cpp/testing/unittest/registar.h> 
 
using namespace NTvmAuth; 
using namespace NTvmAuth::NRoles; 
 
Y_UNIT_TEST_SUITE(Decoder) { 
    const TString BROTLI = NUtils::Base64url2bin("GyMAAAR0Y6ku58ObclAQzDweUSUwbdqc5yOOKgI"); 
    const TString GZIP = NUtils::Base64url2bin("H4sIAAAAAAAA_yrOz01VKEstqkTGCpm5BflFJYl5JQpJOflJgAAAAP__MbeeiSQAAAA"); 
    const TString ZSTD = NUtils::Base64url2bin("KLUv_QBY9AAAwHNvbWUgdmVyeSBpbXBvcnRhbnQgYmxvYgEAc-4IAQAA"); 
 
    Y_UNIT_TEST(Decode) { 
        // Errs 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::Decode( 
                "1:brotli:10000:88839244E8C7C426B20729AF1A13AD792C5FA83C7F2FB6ADCFC60DA1B5EF9603", 
                TString(BROTLI)), 
            yexception, 
            "Decoded blob has bad size: expected 10000, actual 36"); 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::Decode( 
                "1:brotli:36:88839244E8C7C426B20729AF1A13AD792C5FA83C7F2FB6ADCFC60DA1B5EF0000", 
                TString(BROTLI)), 
            yexception, 
            "Decoded blob has bad sha256"); 
 
        // OK 
        TString decoded; 
        UNIT_ASSERT_NO_EXCEPTION( 
            decoded = TDecoder::Decode("", "some veryveryveryvery important blob")); 
        UNIT_ASSERT_VALUES_EQUAL(decoded, "some veryveryveryvery important blob"); 
 
        UNIT_ASSERT_NO_EXCEPTION( 
            decoded = TDecoder::Decode( 
                "1:brotli:36:88839244E8C7C426B20729AF1A13AD792C5FA83C7F2FB6ADCFC60DA1B5EF9603", 
                TString(BROTLI))); 
        UNIT_ASSERT_VALUES_EQUAL(decoded, "some veryveryveryvery important blob"); 
 
        UNIT_ASSERT_NO_EXCEPTION( 
            decoded = TDecoder::Decode( 
                "1:gzip:36:88839244E8C7C426B20729AF1A13AD792C5FA83C7F2FB6ADCFC60DA1B5EF9603", 
                TString(GZIP))); 
        UNIT_ASSERT_VALUES_EQUAL(decoded, "some veryveryveryvery important blob"); 
 
        UNIT_ASSERT_NO_EXCEPTION( 
            decoded = TDecoder::Decode( 
                "1:zstd:36:88839244E8C7C426B20729AF1A13AD792C5FA83C7F2FB6ADCFC60DA1B5EF9603", 
                TString(ZSTD))); 
        UNIT_ASSERT_VALUES_EQUAL(decoded, "some veryveryveryvery important blob"); 
    } 
 
    Y_UNIT_TEST(UnknownCodecs) { 
        for (const TStringBuf codec : {"lz", "lzma", "kek"}) { 
            UNIT_ASSERT_EXCEPTION_CONTAINS( 
                TDecoder::DecodeImpl(codec, ""), 
                yexception, 
                TStringBuilder() << "unknown codec: '" << codec << "'"); 
        } 
    } 
 
    Y_UNIT_TEST(ParseCodec) { 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::ParseCodec("2:kek"), 
            yexception, 
            "unknown codec format version; known: 1; got: 2"); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::ParseCodec("1:::"), 
            yexception, 
            "codec type is empty"); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::ParseCodec("1:some_codec:asd:"), 
            yexception, 
            "decoded blob size is not number"); 
 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::ParseCodec("1:some_codec:789:qwe"), 
            yexception, 
            "sha256 of decoded blob has invalid length: expected 64, got 3"); 
 
        TDecoder::TCodecInfo info; 
        UNIT_ASSERT_NO_EXCEPTION( 
            info = TDecoder::ParseCodec("1:some_codec:789:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")); 
 
        UNIT_ASSERT_VALUES_EQUAL("some_codec", info.Type); 
        UNIT_ASSERT_VALUES_EQUAL(789, info.Size); 
        UNIT_ASSERT_VALUES_EQUAL("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 
                                 info.Sha256); 
    } 
 
    Y_UNIT_TEST(DecodeBrolti) { 
        UNIT_ASSERT_EXCEPTION( 
            TDecoder::DecodeBrolti(""), 
            yexception); 
 
        TString blob; 
        UNIT_ASSERT_NO_EXCEPTION( 
            blob = TDecoder::DecodeBrolti( 
                TString(BROTLI))); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            "some veryveryveryvery important blob", 
            blob); 
    } 
 
    Y_UNIT_TEST(DecodeGzip) { 
        TString blob; 
        UNIT_ASSERT_NO_EXCEPTION(blob = TDecoder::DecodeGzip("")); 
        UNIT_ASSERT_VALUES_EQUAL("", blob); 
 
        UNIT_ASSERT_NO_EXCEPTION( 
            blob = TDecoder::DecodeGzip( 
                TString(GZIP))); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            "some veryveryveryvery important blob", 
            blob); 
    } 
 
    Y_UNIT_TEST(DecodeZstd) { 
        TString blob; 
        UNIT_ASSERT_NO_EXCEPTION(blob = TDecoder::DecodeZstd("")); 
        UNIT_ASSERT_VALUES_EQUAL("", blob); 
 
        UNIT_ASSERT_NO_EXCEPTION( 
            blob = TDecoder::DecodeZstd( 
                TString(ZSTD))); 
 
        UNIT_ASSERT_VALUES_EQUAL( 
            "some veryveryveryvery important blob", 
            blob); 
    } 
 
    Y_UNIT_TEST(VerifySize) { 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::VerifySize("qwerty", 100), 
            yexception, 
            TStringBuilder() << "Decoded blob has bad size: expected 100, actual 6"); 
 
        UNIT_ASSERT_NO_EXCEPTION(TDecoder::VerifySize("qwert", 5)); 
    } 
 
    Y_UNIT_TEST(VerifyChecksum) { 
        UNIT_ASSERT_EXCEPTION_CONTAINS( 
            TDecoder::VerifyChecksum("qwerty", "zzzz"), 
            yexception, 
            "Decoded blob has bad sha256: expected=zzzz," 
            " actual=65E84BE33532FB784C48129675F9EFF3A682B27168C0EA744B2CF58EE02337C5"); 
 
        UNIT_ASSERT_NO_EXCEPTION( 
            TDecoder::VerifyChecksum("qwerty", 
                                     "65E84BE33532FB784C48129675F9EFF3A682B27168C0EA744B2CF58EE02337C5")); 
        UNIT_ASSERT_NO_EXCEPTION( 
            TDecoder::VerifyChecksum("qwerty", 
                                     "65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5")); 
    } 
} 

#include "jwt_token_source.h"

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>

using namespace NYdb;

const TString TestPrivateKeyContent = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC75/JS3rMcLJxv\nFgpOzF5+2gH+Yig3RE2MTl9uwC0BZKAv6foYr7xywQyWIK+W1cBhz8R4LfFmZo2j\nM0aCvdRmNBdW0EDSTnHLxCsFhoQWLVq+bI5f5jzkcoiioUtaEpADPqwgVULVtN/n\nnPJiZ6/dU30C3jmR6+LUgEntUtWt3eq3xQIn5lG3zC1klBY/HxtfH5Hu8xBvwRQT\nJnh3UpPLj8XwSmriDgdrhR7o6umWyVuGrMKlLHmeivlfzjYtfzO1MOIMG8t2/zxG\nR+xb4Vwks73sH1KruH/0/JMXU97npwpe+Um+uXhpldPygGErEia7abyZB2gMpXqr\nWYKMo02NAgMBAAECggEAO0BpC5OYw/4XN/optu4/r91bupTGHKNHlsIR2rDzoBhU\nYLd1evpTQJY6O07EP5pYZx9mUwUdtU4KRJeDGO/1/WJYp7HUdtxwirHpZP0lQn77\nuccuX/QQaHLrPekBgz4ONk+5ZBqukAfQgM7fKYOLk41jgpeDbM2Ggb6QUSsJISEp\nzrwpI/nNT/wn+Hvx4DxrzWU6wF+P8kl77UwPYlTA7GsT+T7eKGVH8xsxmK8pt6lg\nsvlBA5XosWBWUCGLgcBkAY5e4ZWbkdd183o+oMo78id6C+PQPE66PLDtHWfpRRmN\nm6XC03x6NVhnfvfozoWnmS4+e4qj4F/emCHvn0GMywKBgQDLXlj7YPFVXxZpUvg/\nrheVcCTGbNmQJ+4cZXx87huqwqKgkmtOyeWsRc7zYInYgraDrtCuDBCfP//ZzOh0\nLxepYLTPk5eNn/GT+VVrqsy35Ccr60g7Lp/bzb1WxyhcLbo0KX7/6jl0lP+VKtdv\nmto+4mbSBXSM1Y5BVVoVgJ3T/wKBgQDsiSvPRzVi5TTj13x67PFymTMx3HCe2WzH\nJUyepCmVhTm482zW95pv6raDr5CTO6OYpHtc5sTTRhVYEZoEYFTM9Vw8faBtluWG\nBjkRh4cIpoIARMn74YZKj0C/0vdX7SHdyBOU3bgRPHg08Hwu3xReqT1kEPSI/B2V\n4pe5fVrucwKBgQCNFgUxUA3dJjyMES18MDDYUZaRug4tfiYouRdmLGIxUxozv6CG\nZnbZzwxFt+GpvPUV4f+P33rgoCvFU+yoPctyjE6j+0aW0DFucPmb2kBwCu5J/856\nkFwCx3blbwFHAco+SdN7g2kcwgmV2MTg/lMOcU7XwUUcN0Obe7UlWbckzQKBgQDQ\nnXaXHL24GGFaZe4y2JFmujmNy1dEsoye44W9ERpf9h1fwsoGmmCKPp90az5+rIXw\nFXl8CUgk8lXW08db/r4r+ma8Lyx0GzcZyplAnaB5/6j+pazjSxfO4KOBy4Y89Tb+\nTP0AOcCi6ws13bgY+sUTa/5qKA4UVw+c5zlb7nRpgwKBgGXAXhenFw1666482iiN\ncHSgwc4ZHa1oL6aNJR1XWH+aboBSwR+feKHUPeT4jHgzRGo/aCNHD2FE5I8eBv33\nof1kWYjAO0YdzeKrW0rTwfvt9gGg+CS397aWu4cy+mTI+MNfBgeDAIVBeJOJXLlX\nhL8bFAuNNVrCOp79TNnNIsh7\n-----END PRIVATE KEY-----\n";
const TString TestPublicKeyContent = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu+fyUt6zHCycbxYKTsxe\nftoB/mIoN0RNjE5fbsAtAWSgL+n6GK+8csEMliCvltXAYc/EeC3xZmaNozNGgr3U\nZjQXVtBA0k5xy8QrBYaEFi1avmyOX+Y85HKIoqFLWhKQAz6sIFVC1bTf55zyYmev\n3VN9At45kevi1IBJ7VLVrd3qt8UCJ+ZRt8wtZJQWPx8bXx+R7vMQb8EUEyZ4d1KT\ny4/F8Epq4g4Ha4Ue6OrplslbhqzCpSx5nor5X842LX8ztTDiDBvLdv88RkfsW+Fc\nJLO97B9Sq7h/9PyTF1Pe56cKXvlJvrl4aZXT8oBhKxImu2m8mQdoDKV6q1mCjKNN\njQIDAQAB\n-----END PUBLIC KEY-----\n";

Y_UNIT_TEST_SUITE(JwtTokenSourceTest) {
    Y_UNIT_TEST(Encodes) {
        auto source = CreateJwtTokenSource(
            TJwtTokenSourceParams()
                .KeyId("test_key_id")
                .SigningAlgorithm<jwt::algorithm::rs256>("", TestPrivateKeyContent)
                .Issuer("test_issuer")
                .Subject("test_subject")
                .Audience("test_audience")
        );
        const auto now1 = std::chrono::system_clock::from_time_t(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
        auto t = source->GetToken();
        const auto now2 = std::chrono::system_clock::from_time_t(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
        UNIT_ASSERT_VALUES_EQUAL(t.TokenType, "urn:ietf:params:oauth:token-type:jwt");

        jwt::decoded_jwt decoded(t.Token);
        UNIT_ASSERT_VALUES_EQUAL(decoded.get_type(), "JWT");
        UNIT_ASSERT_VALUES_EQUAL(decoded.get_algorithm(), "RS256");
        UNIT_ASSERT_VALUES_EQUAL(decoded.get_key_id(), "test_key_id");
        UNIT_ASSERT(!decoded.has_id());
        UNIT_ASSERT_VALUES_EQUAL(decoded.get_issuer(), "test_issuer");
        UNIT_ASSERT_VALUES_EQUAL(decoded.get_subject(), "test_subject");
        UNIT_ASSERT_VALUES_EQUAL(decoded.get_audience().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*decoded.get_audience().begin(), "test_audience");
        UNIT_ASSERT(decoded.get_issued_at() >= now1);
        UNIT_ASSERT(decoded.get_issued_at() <= now2);
        UNIT_ASSERT(decoded.get_expires_at() >= now1 + std::chrono::hours(1));
        UNIT_ASSERT(decoded.get_expires_at() <= now2 + std::chrono::hours(1));

        const std::string data = decoded.get_header_base64() + "." + decoded.get_payload_base64();
		const std::string signature = decoded.get_signature();
        jwt::algorithm::rs256 alg(TestPublicKeyContent);
        alg.verify(data, signature); // Throws
    }

    Y_UNIT_TEST(BadParams) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateJwtTokenSource(
            TJwtTokenSourceParams()
                .KeyId("test_key_id")
                .Issuer("test_issuer")
                .Subject("test_subject")
                .Audience("test_audience")
        ), std::invalid_argument, "no signing algorithm");

        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateJwtTokenSource(
            TJwtTokenSourceParams()
                .KeyId("test_key_id")
                .SigningAlgorithm<jwt::algorithm::rs256>("", TestPrivateKeyContent)
                .TokenTtl(TDuration::Zero())
        ), std::invalid_argument, "token TTL must be positive");

        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateJwtTokenSource(
            TJwtTokenSourceParams()
                .SigningAlgorithm<jwt::algorithm::rs256>("", TestPrivateKeyContent)
                .AppendAudience("aud")
                .AppendAudience("aud2")
                .AppendAudience("")
        ), std::invalid_argument, "empty audience");
    }
}

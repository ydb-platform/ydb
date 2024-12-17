#include "jwt_token_source.h"
#include <ydb/public/sdk/cpp/client/ydb_types/credentials/oauth2_token_exchange/ut/jwt_check_helper.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/jwt-cpp/include/jwt-cpp/jwt.h>

using namespace NYdb;

extern const TString TestRSAPrivateKeyContent = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC75/JS3rMcLJxv\nFgpOzF5+2gH+Yig3RE2MTl9uwC0BZKAv6foYr7xywQyWIK+W1cBhz8R4LfFmZo2j\nM0aCvdRmNBdW0EDSTnHLxCsFhoQWLVq+bI5f5jzkcoiioUtaEpADPqwgVULVtN/n\nnPJiZ6/dU30C3jmR6+LUgEntUtWt3eq3xQIn5lG3zC1klBY/HxtfH5Hu8xBvwRQT\nJnh3UpPLj8XwSmriDgdrhR7o6umWyVuGrMKlLHmeivlfzjYtfzO1MOIMG8t2/zxG\nR+xb4Vwks73sH1KruH/0/JMXU97npwpe+Um+uXhpldPygGErEia7abyZB2gMpXqr\nWYKMo02NAgMBAAECggEAO0BpC5OYw/4XN/optu4/r91bupTGHKNHlsIR2rDzoBhU\nYLd1evpTQJY6O07EP5pYZx9mUwUdtU4KRJeDGO/1/WJYp7HUdtxwirHpZP0lQn77\nuccuX/QQaHLrPekBgz4ONk+5ZBqukAfQgM7fKYOLk41jgpeDbM2Ggb6QUSsJISEp\nzrwpI/nNT/wn+Hvx4DxrzWU6wF+P8kl77UwPYlTA7GsT+T7eKGVH8xsxmK8pt6lg\nsvlBA5XosWBWUCGLgcBkAY5e4ZWbkdd183o+oMo78id6C+PQPE66PLDtHWfpRRmN\nm6XC03x6NVhnfvfozoWnmS4+e4qj4F/emCHvn0GMywKBgQDLXlj7YPFVXxZpUvg/\nrheVcCTGbNmQJ+4cZXx87huqwqKgkmtOyeWsRc7zYInYgraDrtCuDBCfP//ZzOh0\nLxepYLTPk5eNn/GT+VVrqsy35Ccr60g7Lp/bzb1WxyhcLbo0KX7/6jl0lP+VKtdv\nmto+4mbSBXSM1Y5BVVoVgJ3T/wKBgQDsiSvPRzVi5TTj13x67PFymTMx3HCe2WzH\nJUyepCmVhTm482zW95pv6raDr5CTO6OYpHtc5sTTRhVYEZoEYFTM9Vw8faBtluWG\nBjkRh4cIpoIARMn74YZKj0C/0vdX7SHdyBOU3bgRPHg08Hwu3xReqT1kEPSI/B2V\n4pe5fVrucwKBgQCNFgUxUA3dJjyMES18MDDYUZaRug4tfiYouRdmLGIxUxozv6CG\nZnbZzwxFt+GpvPUV4f+P33rgoCvFU+yoPctyjE6j+0aW0DFucPmb2kBwCu5J/856\nkFwCx3blbwFHAco+SdN7g2kcwgmV2MTg/lMOcU7XwUUcN0Obe7UlWbckzQKBgQDQ\nnXaXHL24GGFaZe4y2JFmujmNy1dEsoye44W9ERpf9h1fwsoGmmCKPp90az5+rIXw\nFXl8CUgk8lXW08db/r4r+ma8Lyx0GzcZyplAnaB5/6j+pazjSxfO4KOBy4Y89Tb+\nTP0AOcCi6ws13bgY+sUTa/5qKA4UVw+c5zlb7nRpgwKBgGXAXhenFw1666482iiN\ncHSgwc4ZHa1oL6aNJR1XWH+aboBSwR+feKHUPeT4jHgzRGo/aCNHD2FE5I8eBv33\nof1kWYjAO0YdzeKrW0rTwfvt9gGg+CS397aWu4cy+mTI+MNfBgeDAIVBeJOJXLlX\nhL8bFAuNNVrCOp79TNnNIsh7\n-----END PRIVATE KEY-----\n";
extern const TString TestRSAPublicKeyContent = "-----BEGIN PUBLIC KEY-----\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAu+fyUt6zHCycbxYKTsxe\nftoB/mIoN0RNjE5fbsAtAWSgL+n6GK+8csEMliCvltXAYc/EeC3xZmaNozNGgr3U\nZjQXVtBA0k5xy8QrBYaEFi1avmyOX+Y85HKIoqFLWhKQAz6sIFVC1bTf55zyYmev\n3VN9At45kevi1IBJ7VLVrd3qt8UCJ+ZRt8wtZJQWPx8bXx+R7vMQb8EUEyZ4d1KT\ny4/F8Epq4g4Ha4Ue6OrplslbhqzCpSx5nor5X842LX8ztTDiDBvLdv88RkfsW+Fc\nJLO97B9Sq7h/9PyTF1Pe56cKXvlJvrl4aZXT8oBhKxImu2m8mQdoDKV6q1mCjKNN\njQIDAQAB\n-----END PUBLIC KEY-----\n";
extern const TString TestECPrivateKeyContent = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIB6fv25gf7P/7fkjW/2kcKICUhHeOygkFeUJ/ylyU3hloAoGCCqGSM49\nAwEHoUQDQgAEvkKy92hpLiT0GEpzFkYBEWWnkAGTTA6141H0oInA9X30eS0RObAa\nmVY8yD39NI7Nj03hBxEa4Z0tOhrq9cW8eg==\n-----END EC PRIVATE KEY-----\n";
extern const TString TestECPublicKeyContent = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEvkKy92hpLiT0GEpzFkYBEWWnkAGT\nTA6141H0oInA9X30eS0RObAamVY8yD39NI7Nj03hBxEa4Z0tOhrq9cW8eg==\n-----END PUBLIC KEY-----\n";
extern const TString TestHMACSecretKeyBase64Content = "VGhlIHdvcmxkIGhhcyBjaGFuZ2VkLgpJIHNlZSBpdCBpbiB0aGUgd2F0ZXIuCkkgZmVlbCBpdCBpbiB0aGUgRWFydGguCkkgc21lbGwgaXQgaW4gdGhlIGFpci4KTXVjaCB0aGF0IG9uY2Ugd2FzIGlzIGxvc3QsCkZvciBub25lIG5vdyBsaXZlIHdobyByZW1lbWJlciBpdC4K";

Y_UNIT_TEST_SUITE(JwtTokenSourceTest) {
    Y_UNIT_TEST(Encodes) {
        auto source = CreateJwtTokenSource(
            TJwtTokenSourceParams()
                .KeyId("test_key_id")
                .SigningAlgorithm<jwt::algorithm::rs256>("", TestRSAPrivateKeyContent)
                .Issuer("test_issuer")
                .Subject("test_subject")
                .Audience("test_audience")
        );

        TJwtCheck check;
        check
            .KeyId("test_key_id")
            .Issuer("test_issuer")
            .Audience("test_audience")
            .Subject("test_subject");

        auto t = source->GetToken();
        UNIT_ASSERT_VALUES_EQUAL(t.TokenType, "urn:ietf:params:oauth:token-type:jwt");
        check.Check(t.Token);
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
                .SigningAlgorithm<jwt::algorithm::rs256>("", TestRSAPrivateKeyContent)
                .TokenTtl(TDuration::Zero())
        ), std::invalid_argument, "token TTL must be positive");

        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateJwtTokenSource(
            TJwtTokenSourceParams()
                .SigningAlgorithm<jwt::algorithm::rs256>("", TestRSAPrivateKeyContent)
                .AppendAudience("aud")
                .AppendAudience("aud2")
                .AppendAudience("")
        ), std::invalid_argument, "empty audience");
    }
}

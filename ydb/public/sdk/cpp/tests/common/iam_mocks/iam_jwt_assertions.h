#pragma once

#include "iam_test_keys.h"

#include <library/cpp/testing/gtest/gtest.h>

#include <jwt-cpp/jwt.h>

#include <chrono>
#include <set>
#include <string>

namespace NYdb::NTest {

inline void AssertIamJwt(const std::string& jwt) {
    ASSERT_FALSE(jwt.empty());

#ifdef YDB_SDK_OSS
    using TDecodedJwt = jwt::decoded_jwt<jwt::traits::kazuho_picojson>;
#else
    using TDecodedJwt = jwt::decoded_jwt;
#endif

    const TDecodedJwt decoded(jwt);
    EXPECT_EQ(decoded.get_algorithm(), "PS256");

    const std::string data = decoded.get_header_base64() + "." + decoded.get_payload_base64();
    const std::string signature = decoded.get_signature();
    jwt::algorithm::ps256 verifier(TestRSAPublicKey, "");
#ifdef YDB_SDK_OSS
    std::error_code ec;
    verifier.verify(data, signature, ec);
    ASSERT_FALSE(ec) << ec.message();
#else
    verifier.verify(data, signature);
#endif

    ASSERT_TRUE(decoded.has_key_id());
    EXPECT_EQ(decoded.get_key_id(), kIamJwtKeyId);

    ASSERT_TRUE(decoded.has_issuer());
    EXPECT_EQ(decoded.get_issuer(), kIamJwtIssuer);

    ASSERT_TRUE(decoded.has_audience());
    EXPECT_EQ(decoded.get_audience(), std::set<std::string>{kIamJwtAudience});

    const auto now = std::chrono::system_clock::now();
    const auto iat = decoded.get_issued_at();
    const auto exp = decoded.get_expires_at();
    EXPECT_GE(iat, now - std::chrono::minutes(10));
    EXPECT_LE(iat, now + std::chrono::minutes(1));
    EXPECT_GT(exp, iat);
    EXPECT_LE(exp - iat, std::chrono::hours(2));
}

} // namespace NYdb::NTest

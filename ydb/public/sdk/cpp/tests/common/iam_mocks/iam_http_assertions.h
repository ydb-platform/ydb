#pragma once

#include "iam_http_mock_server.h"

#include <library/cpp/testing/gtest/gtest.h>

namespace NYdb::NTest {

inline void AssertMetadataRequestShape(const TMetadataServer& server) {
    ASSERT_TRUE(server.HasLastRequest());
    const auto request = server.GetLastRequest();
    EXPECT_TRUE(request.IsValid);
    EXPECT_EQ(request.Method, "GET");
    EXPECT_EQ(request.Path, kMetadataTokenPath);
    EXPECT_EQ(request.MetadataFlavor, kMetadataFlavorValue);
}

} // namespace NYdb::NTest

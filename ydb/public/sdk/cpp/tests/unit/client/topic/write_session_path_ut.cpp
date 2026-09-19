#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_path.h>

#include <library/cpp/testing/unittest/registar.h>

#include <string>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {

    Y_UNIT_TEST_SUITE(WriteSessionPath) {
        Y_UNIT_TEST(AbsoluteDatabase) {
            for (const auto& database : {"/Root/mydb", "/Root/mydb/", "//Root/mydb"}) {
                for (const auto& path : {"Root/mydb/topic", "Root/other/topic", "Root/mydb2/topic", "Root"}) {
                    const auto separator = std::string(database).ends_with('/') ? "" : "/";
                    UNIT_ASSERT_VALUES_EQUAL_C(FullTopicPath(database, path), std::string(database) + separator + path, path);
                }
                for (const auto& path : {"/Root/mydb/topic", "/Root/mydb2/topic", "/Other/topic", "//Root/mydb/topic"}) {
                    UNIT_ASSERT_VALUES_EQUAL_C(FullTopicPath(database, path), path, path);
                }
            }
        }

        Y_UNIT_TEST(RelativeTopicIsJoinedToAbsoluteDatabase) {
            for (const auto& database : {"/Root/mydb", "/Root/mydb/"}) {
                for (const auto& path : {"topic", "mydb/topic", "Root2/topic", "root/topic"}) {
                    UNIT_ASSERT_VALUES_EQUAL_C(FullTopicPath(database, path), std::string("/Root/mydb/") + path, path);
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(FullTopicPath("/", "topic"), "/topic");
        }

        Y_UNIT_TEST(RelativeDatabaseLeavesTopicUnchanged) {
            for (const auto& database : {"", "mydb", "Root/mydb", "Root/mydb/"}) {
                for (const auto& path : {"", "topic", "mydb/topic", "Root/mydb/topic", "Root2/topic", "/Root/mydb/topic", "/Other/topic"}) {
                    UNIT_ASSERT_VALUES_EQUAL_C(FullTopicPath(database, path), path, database);
                }
            }
        }
    } // Y_UNIT_TEST_SUITE(WriteSessionPath)

} // namespace NYdb::inline Dev::NTopic::NTests

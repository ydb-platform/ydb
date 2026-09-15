#include <ydb/public/sdk/cpp/src/client/topic/impl/topic_path.h>

#include <library/cpp/testing/unittest/registar.h>

#include <string>
#include <utility>
#include <vector>

namespace NYdb::inline Dev::NTopic::NTests {

    Y_UNIT_TEST_SUITE(WriteSessionPath) {
        Y_UNIT_TEST(AbsoluteDatabase) {
            for (const auto& database : {"/Root/mydb", "/Root/mydb/", "//Root/mydb"}) {
                for (const auto& [path, expected] : std::vector<std::pair<std::string, std::string>>{
                         {"Root/mydb/topic", "/Root/mydb/topic"},
                         {"Root/other/topic", "/Root/other/topic"},
                         {"Root/mydb2/topic", "/Root/mydb2/topic"},
                         {"Root", "/Root"},
                         {"/Root/mydb/topic", "/Root/mydb/topic"},
                         {"/Root/mydb2/topic", "/Root/mydb2/topic"},
                         {"/Other/topic", "/Other/topic"},
                         {"//Root/mydb/topic", "//Root/mydb/topic"},
                     }) {
                    UNIT_ASSERT_VALUES_EQUAL_C(FullTopicPath(database, path), expected, path);
                }
            }
        }

        Y_UNIT_TEST(RelativeTopicKeepsOldServerCompatibility) {
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

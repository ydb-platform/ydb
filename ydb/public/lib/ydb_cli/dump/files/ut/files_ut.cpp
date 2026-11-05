#include <ydb/public/lib/ydb_cli/dump/files/files.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>

namespace NYdb::NDump::NFiles {

Y_UNIT_TEST_SUITE(BackupFiles) {
    Y_UNIT_TEST(UniqueBasenames) {
        const TFileInfo* files[] = {
            &TableScheme(),
            &Permissions(),
            &Changefeed(),
            &TopicDescription(),
            &CreateTopic(),
            &CreateCoordinationNode(),
            &CreateRateLimiter(),
            &CreateSemaphore(),
            &IncompleteData(),
            &Incomplete(),
            &Empty(),
            &CreateView(),
            &Database(),
            &CreateUser(),
            &CreateGroup(),
            &AlterGroup(),
            &CreateAsyncReplication(),
            &CreateExternalDataSource(),
            &CreateExternalTable(),
            &SystemView(),
            &CreateTransfer(),
        };

        THashSet<TStringBuf> names;
        for (const auto* file : files) {
            const TStringBuf name(file->FileName);
            UNIT_ASSERT(!name.empty());
            UNIT_ASSERT_C(name != "." && name != "..", name);
            UNIT_ASSERT_VALUES_EQUAL_C(name.find_first_of("/\\"), TStringBuf::npos, name);
            UNIT_ASSERT_C(names.insert(name).second, "Duplicate backup file name: " << name);
        }
    }
}

} // namespace NYdb::NDump::NFiles

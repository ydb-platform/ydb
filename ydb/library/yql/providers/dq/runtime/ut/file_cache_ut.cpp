#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/yql/providers/dq/runtime/file_cache.h>
#include <util/system/fs.h>
#include <util/system/file.h>

using namespace NYql;

namespace {
    int fileNo = 1;

    TString GetFile(i64 size) {
        auto file = TFile(std::to_string(fileNo++), CreateAlways | RdWr);
        file.Resize(size);
        return file.GetName();
    }
}

Y_UNIT_TEST_SUITE(TestFileCache) {

Y_UNIT_TEST(Create) {
    NFs::RemoveRecursive("dir_create");
    TFileCache fc("dir_create", 10);

    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 10);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 0);
}

Y_UNIT_TEST(Add) {
    NFs::RemoveRecursive("dir_add");
    TFileCache fc("dir_add", 100);
    fc.AddFile(GetFile(10), "1");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 90);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 10);
    fc.AddFile(GetFile(5), "2");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 85);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 15);
} 

Y_UNIT_TEST(Find) {
    NFs::RemoveRecursive("dir_find");
    TFileCache fc("dir_find", 100);
    fc.AddFile(GetFile(10), "1");
    UNIT_ASSERT(fc.FindFile("1"));
    UNIT_ASSERT(!fc.FindFile("2"));
}

Y_UNIT_TEST(Evict) {
    NFs::RemoveRecursive("dir_evict");
    TFileCache fc("dir_evict", 20);
    fc.AddFile(GetFile(10), "1");
    fc.AddFile(GetFile(15), "2");
    // preserve 2 elements
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), -5);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 25);
    fc.AddFile(GetFile(2), "3");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 3);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 17);
    fc.FindFile("2");
    fc.AddFile(GetFile(4), "4");
    UNIT_ASSERT(!fc.FindFile("3"));
}

Y_UNIT_TEST(AcquireRelease) {
    NFs::RemoveRecursive("dir_acquire");
    TFileCache fc("dir_acquire", 20);
    fc.AddFile(GetFile(10), "1");
    fc.AcquireFile("1");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 10);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 10);
    fc.ReleaseFile("1");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 10);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 10);
}
 
Y_UNIT_TEST(Acquire) {
    NFs::RemoveRecursive("dir_acquire");
    TFileCache fc("dir_acquire", 20);
    fc.AddFile(GetFile(10), "1");
    fc.AddFile(GetFile(15), "2");
    fc.AcquireFile("1");
    fc.AddFile(GetFile(2), "3");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 8);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 12);
    fc.AddFile(GetFile(20), "4");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), -12);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 32);
    fc.ReleaseFile("1");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), -2);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 22);
}

Y_UNIT_TEST(AcquireSingleFile2Times) {
    NFs::RemoveRecursive("dir_acquire");
    TFileCache fc("dir_acquire", 20);
    fc.AddFile(GetFile(10), "1");
    fc.AddFile(GetFile(15), "2");
    fc.AcquireFile("1");
    fc.AcquireFile("1");
    fc.AddFile(GetFile(2), "3");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), 8);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 12);
    fc.AddFile(GetFile(20), "4");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), -12);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 32);
    fc.ReleaseFile("1");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), -12);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 32);
    fc.ReleaseFile("1");
    UNIT_ASSERT_EQUAL(fc.FreeDiskSize(), -2);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 22);
}

Y_UNIT_TEST(ContainsReleased) {
    NFs::RemoveRecursive("contains_released");
    TFileCache fc("contains_released", 20);
    fc.AddFile(GetFile(20), "1");
    fc.AcquireFile("1");
    fc.AddFile(GetFile(15), "2");
    fc.AddFile(GetFile(15), "3");
    THashSet<TString> objects;
    fc.Walk([&](const TString& objectId) {
        objects.insert(objectId);
    });
    UNIT_ASSERT(!objects.contains("1"));
    UNIT_ASSERT(!fc.Contains("1"));
    UNIT_ASSERT(objects.size() == 2);
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 20 + 15 + 15);
    fc.ReleaseFile("1");
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 15 + 15);
}

Y_UNIT_TEST(AddAfterRemoveAcquired) {
    NFs::RemoveRecursive("add_after_remote_acquired");
    TFileCache fc("add_after_remote_acquired", 20);
    fc.AddFile(GetFile(20), "1");
    fc.AcquireFile("1");
    fc.AddFile(GetFile(15), "2");
    fc.AddFile(GetFile(15), "3");
    UNIT_ASSERT(!fc.Contains("1"));
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 20 + 15 + 15);
    fc.AddFile(GetFile(20), "1");
    UNIT_ASSERT_EQUAL(fc.UsedDiskSize(), 20 + 15);
    UNIT_ASSERT(fc.Contains("1"));
    UNIT_ASSERT(fc.Contains("3"));
    UNIT_ASSERT(!fc.Contains("2"));
    fc.ReleaseFile("1");
    UNIT_ASSERT(fc.Contains("1"));
    UNIT_ASSERT(fc.Contains("3"));
    UNIT_ASSERT(!fc.Contains("2"));
}

} // Y_UNIT_TEST_SUITE(TestFileCache) {


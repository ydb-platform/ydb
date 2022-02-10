#include "storage.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/input.h>
#include <util/stream/str.h>

using namespace NYql;
using namespace NThreading;

static TString DATA = "1234567890";
static TString DATA_MD5 = "e807f1fcf82d132f9bb018ca6738a19f";

Y_UNIT_TEST_SUITE(TStorageTests) {

    class TTestDir {
    private:
        TFsPath Path_;

    public:
        TTestDir(const TString& name) {
            Y_ENSURE(name.length() > 0, "have to specify name");
            Y_ENSURE(name.find('.') == TString::npos, "must be simple name");
            Y_ENSURE(name.find('/') == TString::npos, "must be simple name");
            Y_ENSURE(name.find('\\') == TString::npos, "must be simple name");
            Path_ = TFsPath(name);

            Path_.ForceDelete();
            Path_.MkDir();
        }
        ~TTestDir() {
            Path_.ForceDelete();
        }

        TFsPath GetFsPath() const {
            return Path_;
        }
    };

    TFileLinkPtr PutFile(const TString& name, TStorage& storage, const TString& tmpOutName = {}, int sleepSeconds = 0) {
        return storage.Put(name, tmpOutName, DATA_MD5, [&](const TFsPath& storagePath) {
            Sleep(TDuration::Seconds(sleepSeconds));
            TStringInput in(DATA);
            TUnbufferedFileOutput out(storagePath);
            return std::make_pair(TransferData(&in, &out), DATA_MD5);
        });
    }

    TFileLinkPtr PutFileWithSleep(const TString& name, TStorage& storage) {
        return PutFile(name, storage, {}, 1);
    }

    Y_UNIT_TEST(Put) {
        THolder<TStorage> storage = MakeHolder<TStorage>(10, 100);
        TFileLinkPtr fileInStorage = PutFile("test.file", *storage, "somename");
        TFsPath rootPath(fileInStorage->GetPath());
        UNIT_ASSERT(rootPath.Exists());
        UNIT_ASSERT_VALUES_EQUAL(rootPath.GetName(), "somename");
        UNIT_ASSERT_VALUES_EQUAL(fileInStorage->GetStorageFileName(), "test.file");
        UNIT_ASSERT(fileInStorage->GetPath().IsSubpathOf(storage->GetRoot().Fix()));
        UNIT_ASSERT_VALUES_EQUAL(TIFStream(fileInStorage->GetPath()).ReadAll(), DATA);
        UNIT_ASSERT_EQUAL(storage->GetCount(), 1);
        UNIT_ASSERT_EQUAL(storage->GetOccupiedSize(), DATA.size());
        fileInStorage.Reset();
        UNIT_ASSERT(false == rootPath.Exists());
    }

    Y_UNIT_TEST(ParallelPut) {
        THolder<TStorage> storage = MakeHolder<TStorage>(10, 100);
        TThreadPool queue;
        queue.Start(10);

        TVector<TFuture<TFileLinkPtr>> res;
        res.reserve(10);
        for (size_t i = 0; i < 10; ++i) {
            res.emplace_back(Async([&]() {
                return PutFileWithSleep("test.file", *storage);
            }, queue));
        }
        for (auto& f: res) {
            f.Wait();
            TFileLinkPtr fileInStorage = f.GetValue();
            UNIT_ASSERT(fileInStorage->GetPath().Exists());
            UNIT_ASSERT_VALUES_EQUAL(fileInStorage->GetStorageFileName(), "test.file");
            UNIT_ASSERT(fileInStorage->GetPath().IsSubpathOf(storage->GetRoot().Fix()));
            UNIT_ASSERT_VALUES_EQUAL(TIFStream(fileInStorage->GetPath()).ReadAll(), DATA);
            UNIT_ASSERT_EQUAL(storage->GetCount(), 1);
            UNIT_ASSERT_EQUAL(storage->GetOccupiedSize(), DATA.size());
        }
    }

    Y_UNIT_TEST(CleanUp) {
        THolder<TStorage> storage = MakeHolder<TStorage>(10, 100);
        TFileLinkPtr fileInStorage = PutFile("test.file", *storage);
        UNIT_ASSERT(fileInStorage->GetPath().Exists());

        auto rootPath = storage->GetRoot();
        storage.Destroy();
        UNIT_ASSERT(!fileInStorage->GetPath().Exists());
        UNIT_ASSERT(!rootPath.Exists());
    }

    Y_UNIT_TEST(DisplaceByCount) {
        ui64 maxCount = 2;
        THolder<TStorage> storage = MakeHolder<TStorage>(maxCount, 100);

        TFileLinkPtr file1 = PutFile("test1.file", *storage);
        TFileLinkPtr file2 = PutFile("test2.file", *storage);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetOccupiedSize(), 2 * DATA.size());
        UNIT_ASSERT(file1->GetPath().Exists());
        UNIT_ASSERT(file2->GetPath().Exists());

        TFileLinkPtr file3 = PutFileWithSleep("test3.file", *storage);

        // after putting 3rd file we exceeded the limit by count
        // and cleaned up half of the original maxCount files
        UNIT_ASSERT_VALUES_EQUAL(storage->GetCount(), maxCount / 2);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetOccupiedSize(), (maxCount / 2) * DATA.size());

        // despite of file1 and file2 are displaced from storage, we still
        // have temporary hardlinks to it
        UNIT_ASSERT(file1->GetPath().Exists());
        UNIT_ASSERT(file2->GetPath().Exists());
        UNIT_ASSERT(file3->GetPath().Exists());

        TVector<TString> filesInStorage;
        storage->GetRoot().ListNames(filesInStorage);
        UNIT_ASSERT_EQUAL(filesInStorage.size(), 2); // 1 file + 1 hardlink directory

        auto beg = filesInStorage.begin(),
             end = filesInStorage.end();

        // file1 and file2 were displaced
        UNIT_ASSERT(Find(beg, end, file1->GetStorageFileName()) == end);
        UNIT_ASSERT(Find(beg, end, file2->GetStorageFileName()) == end);
        UNIT_ASSERT(Find(beg, end, file3->GetStorageFileName()) != end);
    }

    Y_UNIT_TEST(DisplaceBySize) {
        ui64 maxSize = 25;
        THolder<TStorage> storage = MakeHolder<TStorage>(10, maxSize);

        TFileLinkPtr file1 = PutFile("test1.file", *storage);
        TFileLinkPtr file2 = PutFile("test2.file", *storage);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetOccupiedSize(), 2 * DATA.size());
        UNIT_ASSERT(file1->GetPath().Exists());
        UNIT_ASSERT(file2->GetPath().Exists());

        TFileLinkPtr file3 = PutFileWithSleep("test3.file", *storage);

        // after putting 3rd file we exceeded the limit by size
        // and cleaned up files till we get half of the original maxSize
        UNIT_ASSERT_VALUES_EQUAL(storage->GetCount(), 1);
        UNIT_ASSERT_VALUES_EQUAL(storage->GetOccupiedSize(), DATA.size());

        // despite of file1 and file2 are displaced from storage, we still
        // have temporary hardlinks to it
        UNIT_ASSERT(file1->GetPath().Exists());
        UNIT_ASSERT(file2->GetPath().Exists());
        UNIT_ASSERT(file3->GetPath().Exists());

        TVector<TString> filesInStorage;
        storage->GetRoot().ListNames(filesInStorage);
        UNIT_ASSERT_EQUAL(filesInStorage.size(), 2); // 1 file + 1 hardlink directory

        auto beg = filesInStorage.begin(),
             end = filesInStorage.end();

        // file1 and file2 were displaced
        UNIT_ASSERT(Find(beg, end, file1->GetStorageFileName()) == end);
        UNIT_ASSERT(Find(beg, end, file2->GetStorageFileName()) == end);
        UNIT_ASSERT(Find(beg, end, file3->GetStorageFileName()) != end);
    }

    Y_UNIT_TEST(PersistStorage) {
        TTestDir dir("PersistStorage");
        THolder<TStorage> storage = MakeHolder<TStorage>(100, 100, dir.GetFsPath());
        auto rootPath = storage->GetRoot();

        TFileLinkPtr fileInStorage = PutFile("test.file", *storage);
        UNIT_ASSERT_EQUAL(storage->GetCount(), 1);
        UNIT_ASSERT_EQUAL(storage->GetOccupiedSize(), DATA.size());

        storage.Destroy();
        UNIT_ASSERT(!fileInStorage->GetPath().Exists()); // hardlink was deleted
        UNIT_ASSERT(rootPath.Exists());
        UNIT_ASSERT((rootPath / fileInStorage->GetStorageFileName()).Exists());

        storage.Reset(new TStorage(100, 100, dir.GetFsPath()));
        UNIT_ASSERT_EQUAL(storage->GetCount(), 1);
        UNIT_ASSERT_EQUAL(storage->GetOccupiedSize(), DATA.size());
    }
}

#include "yql_qstorage_file.h"

#include <ydb/library/yql/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <util/folder/tempdir.h>
#include <util/generic/hash_set.h>
#include <util/system/mutex.h>
#include <util/stream/file.h>

namespace NYql {

namespace {

class TWriter : public IQWriter {
public:
    TWriter(TFsPath& path)
        : Path_(path)
        , Storage_(MakeMemoryQStorage())
        , Writer_(Storage_->MakeWriter(""))
    {
    }

    NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) final {
        return Writer_->Put(key, value);
    }

    NThreading::TFuture<void> Commit() final {
        Writer_->Commit().GetValueSync();
        SaveFile(Storage_->MakeIterator("", {}));
        return NThreading::MakeFuture();
    }

private:
    void SaveFile(const IQIteratorPtr& iterator) {
        TFileOutput file(Path_.GetPath());
        for (;;) {
            auto res = iterator->Next().GetValueSync();
            if (!res) {
                break;
            }

            SaveString(file, res->Key.Component);
            SaveString(file, res->Key.Label);
            SaveString(file, res->Value);
        }

        file.Finish();
    }

    void SaveString(TFileOutput& file, const TString& str) {
        ui32 length = str.Size();
        file.Write(&length, sizeof(length));
        file.Write(str.Data(), length);
    }

private:
    const TFsPath Path_;
    const IQStoragePtr Storage_;
    const IQWriterPtr Writer_;
};

class TStorage : public IQStorage {
public:
    TStorage(const TString& folder)
        : Folder_(folder)
    {
        if (!Folder_.IsDefined()) {
            TmpDir_.ConstructInPlace();
            Folder_ = TmpDir_->Path();
        }
    }

    IQWriterPtr MakeWriter(const TString& operationId) const final {
        auto opPath = Folder_ / operationId;
        Y_ENSURE(!opPath.Exists());
        return std::make_shared<TWriter>(opPath);
    }

    IQReaderPtr MakeReader(const TString& operationId) const final {
        auto memory = MakeMemoryQStorage();
        auto opPath = Folder_ / operationId;
        if (opPath.Exists()) {
            LoadFile(opPath, memory);
        }

        return memory->MakeReader("");
    }

    IQIteratorPtr MakeIterator(const TString& operationId, const TQIteratorSettings& settings) const {
        auto memory = MakeMemoryQStorage();
        auto opPath = Folder_ / operationId;
        if (opPath.Exists()) {
            LoadFile(opPath, memory);
        }

        return memory->MakeIterator("", settings);
    }

private:
    void LoadFile(const TFsPath& path, const IQStoragePtr& memory) const {
        auto writer = memory->MakeWriter("");
        TFileInput file(path.GetPath());
        for (;;) {
            TQItemKey key;
            if (!LoadString(file, key.Component)) {
                break;
            }

            Y_ENSURE(LoadString(file, key.Label));
            TString value;
            Y_ENSURE(LoadString(file, value));
            writer->Put(key, value).GetValueSync();
        }

        writer->Commit().GetValueSync();
    }

    bool LoadString(TFileInput& file, TString& str) const {
        ui32 length;
        auto loaded = file.Load(&length, sizeof(length));
        if (!loaded) {
            return false;
        }

        Y_ENSURE(loaded == sizeof(length));
        while (length > 0) {
            char buffer[1024];
            auto toRead = Min<ui32>(sizeof(buffer), length);
            file.LoadOrFail(buffer, toRead);
            length -= toRead;
            str.append(buffer, toRead);
        }

        return true;
    }

private:
    TMaybe<TTempDir> TmpDir_;
    TFsPath Folder_;
};

}

IQStoragePtr MakeFileQStorage(const TString& folder) {
    return std::make_shared<TStorage>(folder);
}

};

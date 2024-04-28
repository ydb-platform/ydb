#include "yql_qstorage_file.h"

#include <ydb/library/yql/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <library/cpp/digest/old_crc/crc.h>

#include <util/folder/tempdir.h>
#include <util/generic/hash_set.h>
#include <util/system/fs.h>
#include <util/system/mutex.h>
#include <util/stream/file.h>

namespace NYql {

namespace {

class TWriter : public IQWriter {
public:
    TWriter(TFsPath& path)
        : Path_(path)
        , Storage_(MakeMemoryQStorage())
        , Writer_(Storage_->MakeWriter("", {}))
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
        TFileOutput dataFile(Path_.GetPath() + ".dat");
        ui64 totalItems = 0;
        ui64 totalBytes = 0;
        ui64 checksum = 0;
        for (;;) {
            auto res = iterator->Next().GetValueSync();
            if (!res) {
                break;
            }

            SaveString(dataFile, res->Key.Component, totalBytes, checksum);
            SaveString(dataFile, res->Key.Label, totalBytes, checksum);
            SaveString(dataFile, res->Value, totalBytes, checksum);
            ++totalItems;
        }

        dataFile.Finish();
        TFileOutput indexFile(Path_.GetPath() + ".idx.tmp");
        indexFile.Write(&totalItems, sizeof(totalItems));
        indexFile.Write(&totalBytes, sizeof(totalBytes));
        indexFile.Write(&checksum, sizeof(checksum));
        if (!NFs::Rename(Path_.GetPath() + ".idx.tmp", Path_.GetPath() + ".idx")) {
            throw yexception() << "can not rename: " << LastSystemErrorText();
        }
    }

    void SaveString(TFileOutput& file, const TString& str, ui64& totalBytes, ui64& checksum) {
        ui32 length = str.Size();
        checksum = crc64(&length, sizeof(length), checksum);
        file.Write(&length, sizeof(length));
        checksum = crc64(str.Data(), length, checksum);
        file.Write(str.Data(), length);
        totalBytes += length;
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

    IQWriterPtr MakeWriter(const TString& operationId, const TQWriterSettings& settings) const final {
        Y_UNUSED(settings);
        auto opPath = Folder_ / operationId;
        return std::make_shared<TWriter>(opPath);
    }

    IQReaderPtr MakeReader(const TString& operationId, const TQReaderSettings& settings) const final {
        Y_UNUSED(settings);
        auto memory = MakeMemoryQStorage();
        LoadFile(operationId, memory);
        return memory->MakeReader("", {});
    }

    IQIteratorPtr MakeIterator(const TString& operationId, const TQIteratorSettings& settings) const {
        auto memory = MakeMemoryQStorage();
        LoadFile(operationId, memory);
        return memory->MakeIterator("", settings);
    }

private:
    void LoadFile(const TString& operationId, const IQStoragePtr& memory) const {
        const TFsPath& indexPath = Folder_ / (operationId + ".idx");
        if (!indexPath.Exists()) {
            return;
        }

        auto writer = memory->MakeWriter("", {});
        TFileInput indexFile(indexPath.GetPath());
        ui64 totalItems, loadedTotalBytes, loadedChecksum;
        indexFile.LoadOrFail(&totalItems, sizeof(totalItems));
        indexFile.LoadOrFail(&loadedTotalBytes, sizeof(loadedTotalBytes));
        indexFile.LoadOrFail(&loadedChecksum, sizeof(loadedChecksum));
        char dummy;
        Y_ENSURE(!indexFile.ReadChar(dummy));
        const TFsPath& dataPath = Folder_ / (operationId + ".dat");
        TFileInput dataFile(dataPath.GetPath());
        ui64 totalBytes = 0, checksum = 0;
        for (ui64 i = 0; i < totalItems; ++i) {
            TQItemKey key;
            LoadString(dataFile, key.Component, totalBytes, checksum, loadedTotalBytes);
            LoadString(dataFile, key.Label, totalBytes, checksum, loadedTotalBytes);
            TString value;
            LoadString(dataFile, value, totalBytes, checksum, loadedTotalBytes);
            writer->Put(key, value).GetValueSync();
            Y_ENSURE(totalBytes <= loadedTotalBytes);
        }

        Y_ENSURE(!indexFile.ReadChar(dummy));
        Y_ENSURE(totalBytes == loadedTotalBytes);
        Y_ENSURE(checksum == loadedChecksum);
        writer->Commit().GetValueSync();
    }

    void LoadString(TFileInput& file, TString& str, ui64& totalBytes, ui64& checksum, ui64 loadedTotalBytes) const {
        ui32 length;
        file.LoadOrFail(&length, sizeof(length));
        Y_ENSURE(totalBytes + length <= loadedTotalBytes);
        checksum = crc64(&length, sizeof(length), checksum);
        str.reserve(length);
        totalBytes += length;
        while (length > 0) {
            char buffer[1024];
            auto toRead = Min<ui32>(sizeof(buffer), length);
            file.LoadOrFail(buffer, toRead);
            length -= toRead;
            str.append(buffer, toRead);
            checksum = crc64(buffer, toRead, checksum);
        }
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

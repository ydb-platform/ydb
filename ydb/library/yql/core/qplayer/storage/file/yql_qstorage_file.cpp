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

class TWriterBase : public IQWriter {
protected:
    TWriterBase(TFsPath& path, TInstant writtenAt)
        : Path_(path)
        , WrittenAt_(writtenAt)
    {
        NFs::Remove(Path_.GetPath() + ".dat");
        NFs::Remove(Path_.GetPath() + ".idx");
        NFs::Remove(Path_.GetPath() + ".idx.tmp");
    }

protected:
    void WriteIndex(ui64 totalItems, ui64 totalBytes, ui64 checksum) const {
        TFileOutput indexFile(Path_.GetPath() + ".idx.tmp");
        indexFile.Write(&WrittenAt_, sizeof(WrittenAt_));
        indexFile.Write(&totalItems, sizeof(totalItems));
        indexFile.Write(&totalBytes, sizeof(totalBytes));
        indexFile.Write(&checksum, sizeof(checksum));
        indexFile.Finish();
        if (!NFs::Rename(Path_.GetPath() + ".idx.tmp", Path_.GetPath() + ".idx")) {
            throw yexception() << "can not rename: " << LastSystemErrorText();
        }
    }

    static void SaveString(TFileOutput& file, const TString& str, ui64& totalBytes, ui64& checksum) {
        ui32 length = str.Size();
        checksum = crc64(&length, sizeof(length), checksum);
        file.Write(&length, sizeof(length));
        checksum = crc64(str.Data(), length, checksum);
        file.Write(str.Data(), length);
        totalBytes += length;
    }

protected:
    const TFsPath Path_;
    const TInstant WrittenAt_;
};

class TBufferedWriter : public TWriterBase {
public:
    TBufferedWriter(TFsPath& path, TInstant writtenAt, const TQWriterSettings& settings)
        : TWriterBase(path, writtenAt)
        , Storage_(MakeMemoryQStorage())
        , Writer_(Storage_->MakeWriter("", settings))
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
        dataFile.Write(&WrittenAt_, sizeof(WrittenAt_));
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
        WriteIndex(totalItems, totalBytes, checksum);
    }

private:
    const IQStoragePtr Storage_;
    const IQWriterPtr Writer_;
};

class TUnbufferedWriter : public TWriterBase {
public:
    TUnbufferedWriter(TFsPath& path, TInstant writtenAt, const TQWriterSettings& settings)
        : TWriterBase(path, writtenAt)
        , Settings_(settings)
    {
        DataFile_.ConstructInPlace(Path_.GetPath() + ".dat");
        DataFile_->Write(&WrittenAt_, sizeof(WrittenAt_));
    }

    ~TUnbufferedWriter() {
        if (!Committed_) {
            DataFile_.Clear();
            NFs::Remove(Path_.GetPath() + ".dat");
        }
    }

    NThreading::TFuture<void> Put(const TQItemKey& key, const TString& value) final {
        with_lock(Mutex_) {
            Y_ENSURE(!Committed_);
            if (!Overflow_) {
                if (Keys_.emplace(key).second) {
                    SaveString(*DataFile_, key.Component, TotalBytes_, Checksum_);
                    SaveString(*DataFile_, key.Label, TotalBytes_, Checksum_);
                    SaveString(*DataFile_, value, TotalBytes_, Checksum_);
                    ++TotalItems_;
                }

                if (Settings_.ItemsLimit && TotalItems_ > *Settings_.ItemsLimit) {
                    Overflow_ = true;
                }

                if (Settings_.BytesLimit && TotalBytes_ > *Settings_.BytesLimit) {
                    Overflow_ = true;
                }
            }

            return NThreading::MakeFuture();
        }
    }

    NThreading::TFuture<void> Commit() final {
        with_lock(Mutex_) {
            if (Overflow_) {
                throw yexception() << "Overflow of qwriter";
            }

            Y_ENSURE(!Committed_);
            Committed_ = true;
            DataFile_->Finish();
            DataFile_.Clear();
            WriteIndex(TotalItems_, TotalBytes_, Checksum_);
            return NThreading::MakeFuture();
        }
    }

private:
    const TQWriterSettings Settings_;
    TMutex Mutex_;
    TMaybe<TFileOutput> DataFile_;
    ui64 TotalItems_ = 0;
    ui64 TotalBytes_ = 0;
    ui64 Checksum_ = 0;
    THashSet<TQItemKey> Keys_;
    bool Committed_ = false;
    bool Overflow_ = false;
};

class TStorage : public IQStorage {
public:
    TStorage(const TString& folder, const TFileQStorageSettings& settings)
        : Folder_(folder)
        , Settings_(settings)
    {
        if (!Folder_.IsDefined()) {
            TmpDir_.ConstructInPlace();
            Folder_ = TmpDir_->Path();
        }
    }

    IQWriterPtr MakeWriter(const TString& operationId, const TQWriterSettings& writerSettings) const final {
        auto opPath = Folder_ / operationId;
        auto writtenAt = writerSettings.WrittenAt.GetOrElse(Now());
        if (Settings_.BufferUntilCommit) {
            return std::make_shared<TBufferedWriter>(opPath, writtenAt, writerSettings);
        } else {
            return std::make_shared<TUnbufferedWriter>(opPath, writtenAt, writerSettings);
        }
    }

    IQReaderPtr MakeReader(const TString& operationId, const TQReaderSettings& readerSettings) const final {
        Y_UNUSED(readerSettings);
        auto memory = MakeMemoryQStorage();
        LoadFile(operationId, memory);
        return memory->MakeReader("", {});
    }

    IQIteratorPtr MakeIterator(const TString& operationId, const TQIteratorSettings& iteratorSettings) const {
        auto memory = MakeMemoryQStorage();
        LoadFile(operationId, memory);
        return memory->MakeIterator("", iteratorSettings);
    }

private:
    void LoadFile(const TString& operationId, const IQStoragePtr& memory) const {
        const TFsPath& indexPath = Folder_ / (operationId + ".idx");
        if (!indexPath.Exists()) {
            return;
        }

        auto writer = memory->MakeWriter("", {});
        TFileInput indexFile(indexPath.GetPath());
        TInstant indexWrittenAt;
        ui64 totalItems, loadedTotalBytes, loadedChecksum;
        indexFile.LoadOrFail(&indexWrittenAt, sizeof(indexWrittenAt));
        indexFile.LoadOrFail(&totalItems, sizeof(totalItems));
        indexFile.LoadOrFail(&loadedTotalBytes, sizeof(loadedTotalBytes));
        indexFile.LoadOrFail(&loadedChecksum, sizeof(loadedChecksum));
        char dummy;
        Y_ENSURE(!indexFile.ReadChar(dummy));
        const TFsPath& dataPath = Folder_ / (operationId + ".dat");
        TFileInput dataFile(dataPath.GetPath());
        TInstant dataWrittenAt;
        dataFile.LoadOrFail(&dataWrittenAt, sizeof(dataWrittenAt));
        Y_ENSURE(indexWrittenAt == dataWrittenAt);
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
    const TFileQStorageSettings Settings_;
};

}

IQStoragePtr MakeFileQStorage(const TString& folder, const TFileQStorageSettings& settings) {
    return std::make_shared<TStorage>(folder, settings);
}

};

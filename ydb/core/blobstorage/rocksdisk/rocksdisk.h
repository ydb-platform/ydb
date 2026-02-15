#pragma once

#include <rocksdb/c.h>

#include <array>
#include <cstddef>

#include <util/generic/string.h>

namespace NKikimr {

struct TRocksDisk {
    enum class EColumnFamily : size_t {
        Default = 0,
        Blobs,
        Blocks,
        GarbageBarriers,
        Count
    };

    TRocksDisk();
    ~TRocksDisk();

    bool Open(const TString& path, TString* error = nullptr);
    bool Put(const TString& key, const TString& value, TString* error = nullptr);
    bool PutSync(const TString& key, const TString& value, TString* error = nullptr);
    bool PutAsync(const TString& key, const TString& value, TString* error = nullptr);
    bool Get(const TString& key, TString* value, TString* error = nullptr) const;
    bool PutSync(EColumnFamily columnFamily, const TString& key, const TString& value, TString* error = nullptr);
    bool PutAsync(EColumnFamily columnFamily, const TString& key, const TString& value, TString* error = nullptr);
    bool Get(EColumnFamily columnFamily, const TString& key, TString* value, TString* error = nullptr) const;
    bool DeleteSync(EColumnFamily columnFamily, const TString& key, TString* error = nullptr);

private:
    bool CheckOpened(TString* error) const;
    bool OpenWithColumnFamilies(const TString& path, TString* error);
    bool PutWithOptions(EColumnFamily columnFamily, const rocksdb_writeoptions_t* writeOptions,
        const TString& key, const TString& value, TString* error);
    bool DeleteWithOptions(EColumnFamily columnFamily, const rocksdb_writeoptions_t* writeOptions,
        const TString& key, TString* error);
    rocksdb_column_family_handle_t* GetColumnFamilyHandle(EColumnFamily columnFamily) const;
    void DestroyColumnFamilies();

    rocksdb_options_t* Options = nullptr;
    rocksdb_writeoptions_t* AsyncWriteOptions = nullptr;
    rocksdb_writeoptions_t* SyncWriteOptions = nullptr;
    rocksdb_readoptions_t* ReadOptions = nullptr;
    rocksdb_t* Db = nullptr;
    std::array<rocksdb_column_family_handle_t*, static_cast<size_t>(EColumnFamily::Count)> ColumnFamilyHandles = {};
};

} // namespace NKikimr

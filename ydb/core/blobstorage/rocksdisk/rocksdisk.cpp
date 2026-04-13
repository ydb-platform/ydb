#include "rocksdisk.h"

namespace NKikimr {

namespace {

using EColumnFamily = TRocksDisk::EColumnFamily;

constexpr std::array<const char*, static_cast<size_t>(EColumnFamily::Count)> ColumnFamilyNames = {
    "default",
    "blobs",
    "blocks",
    "garbage_barriers"
};

bool HandleRocksError(char* err, TString* error) {
    if (err) {
        if (error) {
            *error = err;
        }
        rocksdb_free(err);
        return false;
    }
    if (error) {
        error->clear();
    }
    return true;
}

} // namespace

TRocksDisk::TRocksDisk() {
    Options = rocksdb_options_create();
    AsyncWriteOptions = rocksdb_writeoptions_create();
    SyncWriteOptions = rocksdb_writeoptions_create();
    ReadOptions = rocksdb_readoptions_create();

    rocksdb_options_set_create_if_missing(Options, 1);
    rocksdb_options_set_create_missing_column_families(Options, 1);

    rocksdb_writeoptions_set_sync(AsyncWriteOptions, 0);
    rocksdb_writeoptions_disable_WAL(AsyncWriteOptions, 0);

    rocksdb_writeoptions_set_sync(SyncWriteOptions, 1);
    rocksdb_writeoptions_disable_WAL(SyncWriteOptions, 0);
}

TRocksDisk::~TRocksDisk() {
    DestroyColumnFamilies();
    if (Db) {
        rocksdb_close(Db);
        Db = nullptr;
    }
    if (ReadOptions) {
        rocksdb_readoptions_destroy(ReadOptions);
        ReadOptions = nullptr;
    }
    if (SyncWriteOptions) {
        rocksdb_writeoptions_destroy(SyncWriteOptions);
        SyncWriteOptions = nullptr;
    }
    if (AsyncWriteOptions) {
        rocksdb_writeoptions_destroy(AsyncWriteOptions);
        AsyncWriteOptions = nullptr;
    }
    if (Options) {
        rocksdb_options_destroy(Options);
        Options = nullptr;
    }
}

bool TRocksDisk::CheckOpened(TString* error) const {
    if (!Db) {
        if (error) {
            *error = "DB is not opened";
        }
        return false;
    }
    return true;
}

bool TRocksDisk::Open(const TString& path, TString* error) {
    if (Db) {
        DestroyColumnFamilies();
        rocksdb_close(Db);
        Db = nullptr;
    }

    return OpenWithColumnFamilies(path, error);
}

bool TRocksDisk::Put(const TString& key, const TString& value, TString* error) {
    return PutAsync(EColumnFamily::Blobs, key, value, error);
}

bool TRocksDisk::PutSync(const TString& key, const TString& value, TString* error) {
    return PutSync(EColumnFamily::Blobs, key, value, error);
}

bool TRocksDisk::PutAsync(const TString& key, const TString& value, TString* error) {
    return PutAsync(EColumnFamily::Blobs, key, value, error);
}

bool TRocksDisk::Get(const TString& key, TString* value, TString* error) const {
    return Get(EColumnFamily::Blobs, key, value, error);
}

bool TRocksDisk::PutSync(EColumnFamily columnFamily, const TString& key, const TString& value, TString* error) {
    return PutWithOptions(columnFamily, SyncWriteOptions, key, value, error);
}

bool TRocksDisk::PutAsync(EColumnFamily columnFamily, const TString& key, const TString& value, TString* error) {
    return PutWithOptions(columnFamily, AsyncWriteOptions, key, value, error);
}

bool TRocksDisk::DeleteSync(EColumnFamily columnFamily, const TString& key, TString* error) {
    return DeleteWithOptions(columnFamily, SyncWriteOptions, key, error);
}

bool TRocksDisk::Get(EColumnFamily columnFamily, const TString& key, TString* value, TString* error) const {
    if (!CheckOpened(error)) {
        return false;
    }
    if (!value) {
        if (error) {
            *error = "value pointer is null";
        }
        return false;
    }

    rocksdb_column_family_handle_t* handle = GetColumnFamilyHandle(columnFamily);
    if (!handle) {
        if (error) {
            *error = "column family handle is not initialized";
        }
        return false;
    }

    size_t valueLen = 0;
    char* err = nullptr;
    char* rawValue = rocksdb_get_cf(Db, ReadOptions, handle, key.data(), key.size(), &valueLen, &err);
    if (!HandleRocksError(err, error)) {
        return false;
    }
    if (!rawValue) {
        if (error) {
            *error = "key not found";
        }
        return false;
    }

    *value = TString(rawValue, valueLen);
    rocksdb_free(rawValue);
    if (error) {
        error->clear();
    }
    return true;
}

bool TRocksDisk::OpenWithColumnFamilies(const TString& path, TString* error) {
    std::array<const rocksdb_options_t*, static_cast<size_t>(EColumnFamily::Count)> columnFamilyOptions = {};
    columnFamilyOptions.fill(Options);

    std::array<rocksdb_column_family_handle_t*, static_cast<size_t>(EColumnFamily::Count)> handles = {};

    char* err = nullptr;
    Db = rocksdb_open_column_families(
        Options,
        path.c_str(),
        static_cast<int>(ColumnFamilyNames.size()),
        ColumnFamilyNames.data(),
        columnFamilyOptions.data(),
        handles.data(),
        &err);
    if (!HandleRocksError(err, error)) {
        Db = nullptr;
        return false;
    }

    ColumnFamilyHandles = handles;
    return true;
}

bool TRocksDisk::PutWithOptions(EColumnFamily columnFamily, const rocksdb_writeoptions_t* writeOptions,
        const TString& key, const TString& value, TString* error) {
    if (!CheckOpened(error)) {
        return false;
    }
    if (!writeOptions) {
        if (error) {
            *error = "write options are not initialized";
        }
        return false;
    }

    rocksdb_column_family_handle_t* handle = GetColumnFamilyHandle(columnFamily);
    if (!handle) {
        if (error) {
            *error = "column family handle is not initialized";
        }
        return false;
    }

    char* err = nullptr;
    rocksdb_put_cf(Db, writeOptions, handle, key.data(), key.size(), value.data(), value.size(), &err);
    return HandleRocksError(err, error);
}

bool TRocksDisk::DeleteWithOptions(EColumnFamily columnFamily, const rocksdb_writeoptions_t* writeOptions,
        const TString& key, TString* error) {
    if (!CheckOpened(error)) {
        return false;
    }
    if (!writeOptions) {
        if (error) {
            *error = "write options are not initialized";
        }
        return false;
    }

    rocksdb_column_family_handle_t* handle = GetColumnFamilyHandle(columnFamily);
    if (!handle) {
        if (error) {
            *error = "column family handle is not initialized";
        }
        return false;
    }

    char* err = nullptr;
    rocksdb_delete_cf(Db, writeOptions, handle, key.data(), key.size(), &err);
    return HandleRocksError(err, error);
}

rocksdb_column_family_handle_t* TRocksDisk::GetColumnFamilyHandle(EColumnFamily columnFamily) const {
    const size_t index = static_cast<size_t>(columnFamily);
    if (index >= ColumnFamilyHandles.size()) {
        return nullptr;
    }
    return ColumnFamilyHandles[index];
}

void TRocksDisk::DestroyColumnFamilies() {
    for (rocksdb_column_family_handle_t*& handle : ColumnFamilyHandles) {
        if (handle) {
            rocksdb_column_family_handle_destroy(handle);
            handle = nullptr;
        }
    }
}

} // namespace NKikimr

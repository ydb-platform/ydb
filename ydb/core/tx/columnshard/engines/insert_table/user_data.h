#pragma once
#include "meta.h"

#include <ydb/core/tx/columnshard/common/blob.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap {

class TUserData {
private:
    TInsertedDataMeta Meta;
    YDB_READONLY_DEF(TBlobRange, BlobRange);
    class TBlobStorageGuard {
    private:
        YDB_READONLY_DEF(TString, Data);

    public:
        TBlobStorageGuard(const TString& data)
            : Data(data) {
        }
        ~TBlobStorageGuard();
    };

    std::shared_ptr<TBlobStorageGuard> BlobDataGuard;
    YDB_READONLY(ui64, PathId, 0);
    YDB_READONLY(ui64, SchemaVersion, 0);

public:
    TUserData() = delete;
    TUserData(const ui64 pathId, const TBlobRange& blobRange, const NKikimrTxColumnShard::TLogicalMetadata& proto, const ui64 schemaVersion,
        const std::optional<TString>& blobData);

    static std::shared_ptr<TUserData> Build(const ui64 pathId, const TBlobRange& blobRange, const NKikimrTxColumnShard::TLogicalMetadata& proto, const ui64 schemaVersion,
        const std::optional<TString>& blobData) {
        return std::make_shared<TUserData>(pathId, blobRange, proto, schemaVersion, blobData);
    }

    static std::shared_ptr<TUserData> Build(const ui64 pathId, const TUnifiedBlobId& blobId, const NKikimrTxColumnShard::TLogicalMetadata& proto, const ui64 schemaVersion,
        const std::optional<TString>& blobData) {
        return std::make_shared<TUserData>(pathId, TBlobRange(blobId), proto, schemaVersion, blobData);
    }

    std::optional<TString> GetBlobData() const {
        if (BlobDataGuard) {
            return BlobDataGuard->GetData();
        } else {
            return std::nullopt;
        }
    }

    ui64 GetTxVolume() const {
        return Meta.GetTxVolume() + sizeof(TBlobRange);
    }

    const TInsertedDataMeta& GetMeta() const {
        return Meta;
    }
};

class TUserDataContainer {
protected:
    std::shared_ptr<TUserData> UserData;

public:
    TUserDataContainer(const std::shared_ptr<TUserData>& userData)
        : UserData(userData) {
        AFL_VERIFY(UserData);
    }

    ui64 GetSchemaVersion() const {
        return UserData->GetSchemaVersion();
    }

    ui32 BlobSize() const {
        return GetBlobRange().Size;
    }

    ui32 GetTxVolume() const {
        return UserData->GetTxVolume();
    }

    ui64 GetPathId() const {
        return UserData->GetPathId();
    }

    const TBlobRange& GetBlobRange() const {
        return UserData->GetBlobRange();
    }

    std::optional<TString> GetBlobData() const {
        return UserData->GetBlobData();
    }

    const TInsertedDataMeta& GetMeta() const {
        return UserData->GetMeta();
    }
};

}   // namespace NKikimr::NOlap

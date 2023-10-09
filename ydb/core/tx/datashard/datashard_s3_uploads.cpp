#include "datashard_s3_uploads.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

bool TS3UploadsManager::Load(NIceDb::TNiceDb& db) {
    using Schema = TDataShard::Schema;

    {
        auto rowset = db.Table<Schema::S3Uploads>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui64 txId = rowset.GetValue<Schema::S3Uploads::TxId>();
            const TString uploadId = rowset.GetValue<Schema::S3Uploads::UploadId>();

            auto res = Uploads.emplace(txId, TS3Upload(uploadId));
            Y_VERIFY_S(res.second, "Unexpected duplicate s3 upload"
                << ": txId# " << txId
                << ", uploadId# " << uploadId);

            auto& upload = res.first->second;

            upload.Status = rowset.GetValue<Schema::S3Uploads::Status>();
            if (rowset.HaveValue<Schema::S3Uploads::Error>()) {
                upload.Error = rowset.GetValue<Schema::S3Uploads::Error>();
            }

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    if (db.HaveTable<Schema::S3UploadedParts>()) {
        auto rowset = db.Table<Schema::S3UploadedParts>().Range().Select();
        if (!rowset.IsReady()) {
            return false;
        }

        while (!rowset.EndOfSet()) {
            const ui64 txId = rowset.GetValue<Schema::S3UploadedParts::TxId>();

            auto it = Uploads.find(txId);
            Y_VERIFY_S(it != Uploads.end(), "Unknown s3 upload part"
                << ": txId# " << txId);

            auto& parts = it->second.Parts;

            const ui32 partNumber = rowset.GetValue<Schema::S3UploadedParts::PartNumber>();
            if (parts.size() < partNumber) {
                parts.resize(partNumber);
            }

            parts[partNumber - 1] = rowset.GetValue<Schema::S3UploadedParts::ETag>();

            if (!rowset.Next()) {
                return false;
            }
        }
    }

    return true;
}

void TS3UploadsManager::Reset() {
    Uploads.clear();
}

const TS3Upload* TS3UploadsManager::Find(ui64 txId) const {
    auto it = Uploads.find(txId);
    if (it == Uploads.end()) {
        return nullptr;
    }

    return &it->second;
}

const TS3Upload& TS3UploadsManager::Add(NIceDb::TNiceDb& db, ui64 txId, const TString& uploadId) {
    using Schema = TDataShard::Schema;

    Y_ABORT_UNLESS(!Uploads.contains(txId));
    auto res = Uploads.emplace(txId, TS3Upload(uploadId));

    db.Table<Schema::S3Uploads>().Key(txId).Update<Schema::S3Uploads::UploadId>(uploadId);

    return res.first->second;
}

const TS3Upload& TS3UploadsManager::ChangeStatus(NIceDb::TNiceDb& db, ui64 txId, TS3Upload::EStatus status,
        TMaybe<TString>&& error, TVector<TString>&& parts)
{
    using Schema = TDataShard::Schema;

    auto it = Uploads.find(txId);
    Y_ABORT_UNLESS(it != Uploads.end());

    auto& upload = it->second;
    upload.Status = status;
    upload.Error = std::move(error);
    upload.Parts = std::move(parts);

    db.Table<Schema::S3Uploads>().Key(txId).Update<Schema::S3Uploads::Status>(upload.Status);

    if (upload.Error) {
        db.Table<Schema::S3Uploads>().Key(txId).Update<Schema::S3Uploads::Error>(*upload.Error);
    }

    for (ui32 partIndex = 0; partIndex < upload.Parts.size(); ++partIndex) {
        const ui32 partNumber = partIndex + 1;
        const auto& eTag = upload.Parts.at(partIndex);

        db.Table<Schema::S3UploadedParts>().Key(txId, partNumber).Update(
            NIceDb::TUpdate<Schema::S3UploadedParts::ETag>(eTag)
        );
    }

    return upload;
}

}   // namespace NDataShard
}   // namespace NKikimr

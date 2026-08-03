#include "object_key.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/s3_object_key.h>
#include <ydb/core/protos/config.pb.h>

#include <util/digest/multi.h>
#include <util/string/builder.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

ui32 TObjectKey::GetChannelForWriting() {
    if (HasAppData() && !AppData()->ColumnShardConfig.GetEnableTieringObjectKeyTree()) {
        return FlatLayoutChannel;
    }

    return TreeLayoutChannel;
}

TString TObjectKey::Make(const TLogoBlobID& blobId) {
    if (blobId.Channel() != TreeLayoutChannel) {
        return blobId.ToString();
    }

    const size_t hash = MultiHash(blobId.TabletID(), blobId.Generation(), blobId.Step(), blobId.Cookie());
    return TStringBuilder() << blobId.TabletID() << '/' << blobId.Generation() << '/' << MakeS3KeyFanout(hash) << '/' << blobId.ToString();
}

bool TObjectKey::Parse(const TString& key, TLogoBlobID& blobId, TString& error) {
    TStringBuf blobIdStr(key);
    if (const size_t pos = blobIdStr.rfind('/'); pos != TStringBuf::npos) {
        blobIdStr = blobIdStr.SubStr(pos + 1);
    }

    TLogoBlobID parsed;
    if (!TLogoBlobID::Parse(parsed, TString(blobIdStr), error)) {
        return false;
    }

    if (Make(parsed) != key) {
        error = TStringBuilder() << "object key does not match the blob id it contains";
        return false;
    }

    blobId = parsed;
    return true;
}

}   // namespace NKikimr::NOlap::NBlobOperations::NTier

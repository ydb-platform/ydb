#include "local_kikhouse_snapshot_id.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NGRpcService {

namespace {

constexpr TStringBuf SnapshotUriPrefix = "snapshot:///";

} // namespace

TString TKikhouseSnapshotId::ToUri() const {
    return TStringBuilder() << SnapshotUriPrefix << Step << "/" << TxId;
}

bool TKikhouseSnapshotId::Parse(const TString& uri) {
    if (!uri.StartsWith(SnapshotUriPrefix)) {
        return false;
    }

    TStringBuf left;
    TStringBuf right;
    TStringBuf tail(uri.data() + SnapshotUriPrefix.size(), uri.size() - SnapshotUriPrefix.size());
    if (!tail.TrySplit('/', left, right)) {
        return false;
    }

    return TryFromString(left, Step) && TryFromString(right, TxId);
}

} // namespace NKikimr::NGRpcService

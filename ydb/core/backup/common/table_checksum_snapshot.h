#pragma once

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NBackup {

// Old binaries expire these pins; new binaries release them after the owner finishes
inline constexpr TDuration TableChecksumSnapshotTimeout = TDuration::Minutes(5);

struct TTableChecksumSnapshotOwner {
    ui64 SchemeShardId;
    ui64 OperationId;
    bool Importing;

    TString ToString() const {
        return TStringBuilder() << "__table_checksum/" << SchemeShardId << '/' << (Importing ? "import/" : "export/")
                                << OperationId;
    }

    static TMaybe<TTableChecksumSnapshotOwner> Parse(TStringBuf name) {
        if (!name.SkipPrefix("__table_checksum/")) {
            return {};
        }
        TTableChecksumSnapshotOwner owner{};
        const auto shard = name.NextTok('/');
        const auto kind = name.NextTok('/');
        if (!TryFromString(shard, owner.SchemeShardId) || !owner.SchemeShardId ||
            (kind != "import" && kind != "export") || !TryFromString(name, owner.OperationId) || !owner.OperationId) {
            return {};
        }
        owner.Importing = kind == "import";
        return owner;
    }
};

} // namespace NKikimr::NBackup

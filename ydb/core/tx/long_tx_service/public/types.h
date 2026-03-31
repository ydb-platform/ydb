#pragma once

#include <util/generic/hash.h>
#include <ydb/core/base/row_version.h>
#include <ydb/core/util/ulid.h>
#include <ydb/core/protos/long_tx_service.pb.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {
namespace NLongTxService {

    struct TLongTxId {
        TULID UniqueId;
        ui32 NodeId;
        TRowVersion Snapshot;

        TLongTxId()
            : UniqueId(TULID::Min())
            , NodeId(0)
            , Snapshot(TRowVersion::Min())
        { }

        TLongTxId(const TULID& uniqueId, ui32 nodeId, const TRowVersion& snapshot = TRowVersion::Min())
            : UniqueId(uniqueId)
            , NodeId(nodeId)
            , Snapshot(snapshot)
        { }

        explicit TLongTxId(const TRowVersion& snapshot)
            : UniqueId(TULID::Min())
            , NodeId(0)
            , Snapshot(snapshot)
        { }

        bool operator==(const TLongTxId& rhs) const noexcept {
            return (
                UniqueId == rhs.UniqueId &&
                NodeId == rhs.NodeId &&
                Snapshot == rhs.Snapshot);
        }

        bool operator!=(const TLongTxId& rhs) const noexcept {
            return !(*this == rhs);
        }

        bool IsWritable() const {
            return NodeId != 0;
        }

        bool IsReadable() const {
            return bool(Snapshot);
        }

        bool ParseString(TStringBuf buf, TString* errStr = nullptr) noexcept;
        TString ToString() const;

        static TLongTxId FromProto(const NKikimrLongTxService::TLongTxId& proto) noexcept;
        void ParseProto(const NKikimrLongTxService::TLongTxId& proto) noexcept;
        void ToProto(NKikimrLongTxService::TLongTxId* proto) const;
        NKikimrLongTxService::TLongTxId ToProto() const;
    };

    struct TLockInfo {
        ui64 LockId;
        ui32 LockNodeId;

        TLockInfo(ui64 lockId, ui32 lockNodeId)
            : LockId(lockId)
            , LockNodeId(lockNodeId)
        {}

        bool operator==(const TLockInfo& other) const noexcept {
            return LockId == other.LockId && LockNodeId == other.LockNodeId;
        }

        bool operator!=(const TLockInfo& other) const noexcept {
            return !(*this == other);
        }
    };

    struct TWaitEdgeId {
        NActors::TActorId OwnerId;
        ui64 RequestId;

        TWaitEdgeId(const NActors::TActorId& ownerId, ui64 requestId)
            : OwnerId(ownerId), RequestId(requestId)
        {}

        bool operator==(const TWaitEdgeId& other) const noexcept {
            return OwnerId == other.OwnerId && RequestId == other.RequestId;
        }

        bool operator!=(const TWaitEdgeId& other) const noexcept {
            return !(*this == other);
        }
    };

} // namespace NLongTxService
} // namespace NKikimr

template <>
struct THash<NKikimr::NLongTxService::TWaitEdgeId> {
    inline ui64 operator()(const NKikimr::NLongTxService::TWaitEdgeId& x) const {
        return CombineHashes(x.OwnerId.Hash(), std::hash<ui64>{}(x.RequestId));
    }
};

namespace std {
    template<>
    struct hash<NKikimr::NLongTxService::TWaitEdgeId>
        : THash<NKikimr::NLongTxService::TWaitEdgeId>
    {};
}

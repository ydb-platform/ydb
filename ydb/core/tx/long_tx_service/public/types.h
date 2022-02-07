#pragma once
#include <ydb/core/base/row_version.h>
#include <ydb/core/util/ulid.h>
#include <ydb/core/protos/long_tx_service.pb.h>

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

} // namespace NLongTxService
} // namespace NKikimr

#include "types.h"

#include <library/cpp/cgiparam/cgiparam.h>
#include <library/cpp/uri/uri.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NLongTxService {

    namespace {
        bool TryParseTxId(TStringBuf buf, ui64& txId) {
            if (buf == "max") {
                txId = Max<ui64>();
                return true;
            }
            return TryFromString(buf, txId);
        }
    }

    bool TLongTxId::ParseString(TStringBuf buf, TString* errStr) noexcept {
        using namespace NUri;

        TUri uri;
        auto status = uri.Parse(buf, TFeature::FeaturesDefault | TFeature::FeatureSchemeFlexible);
        if (status != TState::ParsedOK) {
            if (errStr) {
                *errStr = "Cannot parse the given uri";
            }
            return false;
        }
        if (uri.GetField(TField::FieldScheme) != "ydb") {
            if (errStr) {
                *errStr = "Expected uri scheme ydb";
            }
            return false;
        }
        if (uri.GetField(TField::FieldHost) != "long-tx") {
            if (errStr) {
                *errStr = "Expected uri type long-tx";
            }
            return false;
        }

        bool readOnly = false;

        auto path = uri.GetField(TField::FieldPath);
        if (path.empty() || path[0] != '/') {
            if (errStr) {
                *errStr = "Unexpected empty uri path";
            }
            return false;
        }
        path = path.substr(1);
        if (!UniqueId.ParseString(path)) {
            if (path != "read-only") {
                if (errStr) {
                    *errStr = "Invalid transaction id";
                }
                return false;
            } else {
                readOnly = true;
                UniqueId = TULID::Min();
            }
        }

        NodeId = 0;
        Snapshot = { 0, 0 };

        auto query = uri.GetField(TField::FieldQuery);
        if (query) {
            TCgiParameters params(query);
            for (auto it : params) {
                if (it.first == "node_id" && !readOnly) {
                    if (!TryFromString(it.second, NodeId)) {
                        if (errStr) {
                            *errStr = "Invalid node_id value";
                        }
                        return false;
                    }
                } else if (it.first == "snapshot") {
                    auto pos = it.second.find(':');
                    if (pos == TString::npos ||
                        !TryFromString(it.second.substr(0, pos), Snapshot.Step) ||
                        !TryParseTxId(it.second.substr(pos + 1), Snapshot.TxId))
                    {
                        if (errStr) {
                            *errStr = "Invalid snapshot value";
                        }
                        return false;
                    }
                } else {
                    if (errStr) {
                        *errStr = "Unsupported parameter '" + it.first + "'";
                    }
                    return false;
                }
            }
        }

        if (!readOnly && NodeId == 0) {
            if (errStr) {
                *errStr = "Missing node_id value";
            }
            return false;
        }

        if (readOnly && !Snapshot) {
            if (errStr) {
                *errStr = "Missing snapshot value";
            }
            return false;
        }

        return true;
    }

    TString TLongTxId::ToString() const {
        TStringBuilder builder;
        builder << "ydb://long-tx/";
        size_t args = 0;
        if (NodeId) {
            builder << UniqueId.ToString();
            builder << "?node_id=";
            builder << NodeId;
            ++args;
        } else {
            builder << "read-only";
        }
        if (Snapshot) {
            builder << (args ? '&' : '?');
            builder << "snapshot=";
            builder << Snapshot.Step;
            builder << "%3A";
            if (Snapshot.TxId == Max<ui64>()) {
                builder << "max";
            } else {
                builder << Snapshot.TxId;
            }
            ++args;
        }
        return std::move(builder);
    }

    TLongTxId TLongTxId::FromProto(const NKikimrLongTxService::TLongTxId& proto) noexcept {
        TLongTxId res;
        res.ParseProto(proto);
        return res;
    }

    void TLongTxId::ParseProto(const NKikimrLongTxService::TLongTxId& proto) noexcept {
        if (proto.HasNodeId()) {
            Y_ABORT_UNLESS(proto.HasUniqueId() && proto.GetUniqueId().size() == 16,
                "Unexpected malformed UniqueId in TLongTxId");
            UniqueId.SetBinary(proto.GetUniqueId());
            NodeId = proto.GetNodeId();
        } else {
            UniqueId = TULID::Min();
            NodeId = 0;
        }
        Snapshot.Step = proto.GetSnapshotStep();
        Snapshot.TxId = proto.GetSnapshotTxId();
    }

    void TLongTxId::ToProto(NKikimrLongTxService::TLongTxId* proto) const {
        if (NodeId) {
            proto->SetUniqueId(UniqueId.ToBinary());
            proto->SetNodeId(NodeId);
        }
        if (Snapshot) {
            proto->SetSnapshotStep(Snapshot.Step);
            proto->SetSnapshotTxId(Snapshot.TxId);
        }
    }

    NKikimrLongTxService::TLongTxId TLongTxId::ToProto() const {
        NKikimrLongTxService::TLongTxId proto;
        ToProto(&proto);
        return proto;
    }

} // namespace NLongTxService
} // namespace NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::NLongTxService::TLongTxId, out, txId) {
    out << txId.ToString();
}

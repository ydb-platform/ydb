#include "receipt.h"

#include <ydb/services/sqs_topic/protos/receipt/receipt.pb.h>

#include <library/cpp/string_utils/base64/base64.h>

namespace NKikimr::NSqsTopic::V1 {

    TString SerializeReceipt(const NPQ::NMLP::TMessageId& pos) {
        Ydb::SqsTopic::Receipt::TReceipt proto;
        proto.SetPartition(pos.PartitionId);
        proto.SetOffset(pos.Offset);
        TString protoStr;
        bool ok = proto.SerializeToString(&protoStr);
        Y_ASSERT(ok && "Receipt serialization failed");
        return Base64Encode(protoStr);
    }

    std::expected<NPQ::NMLP::TMessageId, TString> DeserializeReceipt(TStringBuf receipt) {
        try {
            const TString protoStr = Base64Decode(receipt);
            Ydb::SqsTopic::Receipt::TReceipt proto;
            bool ok = proto.ParseFromString(protoStr);
            if (!ok) {
                return std::unexpected("Protobuf deserialization failed");
            }
            if (!proto.HasPartition()) {
                return std::unexpected("Invalid receipt, ParitionId missing");
            }
            if (!proto.HasOffset()) {
                return std::unexpected("Invalid receipt, Offset missing");
            }
            return NPQ::NMLP::TMessageId{proto.GetPartition(), proto.GetOffset()};
        } catch (const std::exception& e) {
            return std::unexpected(e.what());
        }
    }

} // namespace NKikimr::NSqsTopic::V1

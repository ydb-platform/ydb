#pragma once

#include <util/generic/fwd.h>

namespace Ydb {
    namespace Topic {
        class Consumer;
        class DescribeTopicResult;
    }
    class StatusIds;
    enum StatusIds_StatusCode : int;
}

namespace NKikimrSchemeOp {
    class TPersQueueGroupDescription;
    class TDirEntry;
}

namespace NYql {
    class TIssue;
}

namespace NKikimrPQ {
    class TPQTabletConfig_TConsumer;
    class TPQConfig;
}

namespace NKikimr {

<<<<<<< HEAD
bool FillConsumer(Ydb::Topic::Consumer& out, const NKikimrPQ::TPQTabletConfig_TConsumer& in, Ydb::StatusIds_StatusCode& status, TString& error, bool checkServiceType = true);
=======
// Resolves consumer ServiceType for describe responses (Topic / PQv1).
// - HasServiceType → that value
// - !checkServiceType and no ServiceType → empty string (ok)
// - checkServiceType and no ServiceType:
//     DisallowDefaultClientServiceType → false, error "service type must be set..."
//     else → DefaultClientServiceType name
bool ResolveConsumerServiceType(
    const NKikimrPQ::TPQTabletConfig_TConsumer& consumer,
    const NKikimrPQ::TPQConfig& pqConfig,
    bool checkServiceType,
    TString& outServiceType,
    TString& error);

bool FillConsumer(Ydb::Topic::Consumer& out, const NKikimrPQ::TPQTabletConfig& config, const NKikimrPQ::TPQTabletConfig_TConsumer& in, Ydb::StatusIds_StatusCode& status, TString& error, bool checkServiceType = true);
>>>>>>> f3674463da7 (Reduce msgbus usage in PQ tests: create/alter/delete via Topic/PersQueue SDK (#49687))
bool FillTopicDescription(Ydb::Topic::DescribeTopicResult& out, const NKikimrSchemeOp::TPersQueueGroupDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry, const TMaybe<TString>& cdcName,
    Ydb::StatusIds_StatusCode& status, TString& error);

} // namespace NKikimr

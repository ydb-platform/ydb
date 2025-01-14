#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

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

namespace NActors {
    struct TActorContext;
}

namespace NKikimr {

bool FillConsumer(Ydb::Topic::Consumer *rr, const NKikimrPQ::TPQTabletConfig::TConsumer& consumer,
    Ydb::StatusIds_StatusCode& status, TString& error);
bool FillConsumer(Ydb::Topic::Consumer* rr, const NKikimrPQ::TPQTabletConfig::TConsumer& consumer,
    const NActors::TActorContext& ctx, Ydb::StatusIds_StatusCode& status, TString& error);
bool FillTopicDescription(Ydb::Topic::DescribeTopicResult& out, const NKikimrSchemeOp::TPersQueueGroupDescription& in, const NKikimrPQ::TPQConfig& pqConfig,
    const NKikimrSchemeOp::TDirEntry &fromDirEntry, const TMaybe<TString>& cdcName,
    bool EnableTopicSplitMerge, NYql::TIssue& issue, const NActors::TActorContext& ctx, const TString& consumer, bool includeStats, bool includeLocation);

} // namespace NKikimr

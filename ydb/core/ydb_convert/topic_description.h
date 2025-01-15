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

namespace NActors {
    struct TActorContext;
}

namespace NKikimrPQ {
    class TPQTabletConfig_TConsumer;
    class TPQConfig;
}

namespace NKikimr {

bool FillConsumer(Ydb::Topic::Consumer& rr, const NKikimrPQ::TPQTabletConfig_TConsumer& consumer, Ydb::StatusIds_StatusCode& status, TString& error);
void FillTopicDescription(Ydb::Topic::DescribeTopicResult& out, const NKikimrSchemeOp::TPersQueueGroupDescription& in,
    const NKikimrSchemeOp::TDirEntry &fromDirEntry, const TMaybe<TString>& cdcName);

} // namespace NKikimr

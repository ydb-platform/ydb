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

bool FillConsumer(Ydb::Topic::Consumer& out, const NKikimrPQ::TPQTabletConfig_TConsumer& in, Ydb::StatusIds_StatusCode& status, TString& error);
bool FillTopicDescription(Ydb::Topic::DescribeTopicResult& out, const NKikimrSchemeOp::TPersQueueGroupDescription& inDesc,
    const NKikimrSchemeOp::TDirEntry& inDirEntry, const TMaybe<TString>& cdcName,
    Ydb::StatusIds_StatusCode& status, TString& error);

} // namespace NKikimr

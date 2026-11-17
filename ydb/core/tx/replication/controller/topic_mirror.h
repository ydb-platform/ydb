#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

#include <util/generic/string.h>

namespace NKikimrReplication {
    class TConnectionParams;
}

namespace NYdb::inline Dev::NTopic {
    class TTopicDescription;
}

namespace Ydb::Topic {
    class CreateTopicRequest;
}

namespace NKikimr::NReplication::NController {

Ydb::Topic::CreateTopicRequest BuildTopicMirrorRequest(const NYdb::NTopic::TTopicDescription& description,
    const TString& consumer);

struct TTopicMirrorSettings {
    NKikimrPQ::TMirrorPartitionConfig Config;
    TString Error;

    TTopicMirrorSettings() = default;
    TTopicMirrorSettings(const NKikimrReplication::TConnectionParams& params,
        const TString& srcPath, const TString& consumer);
};

}

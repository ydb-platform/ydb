#pragma once

#include <util/generic/string.h>

#include <memory>

namespace NKikimrPQ {
    class TMirrorPartitionConfig;
}

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
    std::unique_ptr<NKikimrPQ::TMirrorPartitionConfig> Config;
    TString Error;

    TTopicMirrorSettings();
    TTopicMirrorSettings(TTopicMirrorSettings&&) noexcept;
    ~TTopicMirrorSettings();
    TTopicMirrorSettings(const NKikimrReplication::TConnectionParams& params,
        const TString& srcPath, const TString& consumer);
};

}

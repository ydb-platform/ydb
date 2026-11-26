#pragma once

#include <util/generic/string.h>

#include <memory>
#include <optional>

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
    class DescribeTopicResult;
}

namespace NKikimr::NReplication::NController {

Ydb::Topic::CreateTopicRequest BuildTopicMirrorRequest(const NYdb::NTopic::TTopicDescription& description,
    const TString& consumer);

std::optional<ui64> GetTopicDataLagOffsets(const Ydb::Topic::DescribeTopicResult& source,
    const Ydb::Topic::DescribeTopicResult& destination);

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

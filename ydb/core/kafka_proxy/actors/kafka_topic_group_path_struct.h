#pragma once

#include <util/generic/cast.h>
#include <util/digest/numeric.h>

namespace NKafka {
    struct TTopicGroupIdAndPath {
        TString GroupId;
        TString TopicPath;

        bool operator==(const TTopicGroupIdAndPath& topicGroupIdAndPath) const {
            return GroupId == topicGroupIdAndPath.GroupId && TopicPath == topicGroupIdAndPath.TopicPath;
        }
    };

    struct TTopicGroupIdAndPathHash { size_t operator()(const TTopicGroupIdAndPath& alterTopicRequest) const { return CombineHashes(std::hash<TString>()(alterTopicRequest.GroupId), std::hash<TString>()(alterTopicRequest.TopicPath)); } };

}

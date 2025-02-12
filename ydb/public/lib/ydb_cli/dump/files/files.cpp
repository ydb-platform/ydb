#include "files.h"

namespace NYdb::NDump::NFiles {

enum EFilesType {
    SCHEME = 0,
    PERMISSIONS,
    CHANGEFEED_DESCRIPTION,
    TOPIC_DESCRIPTION,
    CREATE_TOPIC,
    CREATE_COORDINATION_NODE,
    INCOMPLETE_DATA,
    INCOMPLETE,
    EMPTY,
    CREATE_VIEW,
};

static constexpr TFileInfo FILES_INFO[] = {
    {"scheme.pb", "scheme"},
    {"permissions.pb", "ACL"},
    {"changefeed_description.pb", "changefeed"},
    {"topic_description.pb", "topic"},
    {"create_topic.pb", "topic"},
    {"create_coordination_node.pb", "coordination node"},
    {"incomplete.csv", "incomplete"},
    {"incomplete", "incomplete"},
    {"empty_dir", "empty_dir"},
    {"create_view.sql", "view"},
};

const TFileInfo& TableScheme() {
    return FILES_INFO[SCHEME];
}

const TFileInfo& Permissions() {
    return FILES_INFO[PERMISSIONS];
}

const TFileInfo& Changefeed() {
    return FILES_INFO[CHANGEFEED_DESCRIPTION];
}

const TFileInfo& TopicDescription() {
    return FILES_INFO[TOPIC_DESCRIPTION];
}

const TFileInfo& CreateTopic() {
    return FILES_INFO[CREATE_TOPIC];
}

const TFileInfo& CreateCoordinationNode() {
    return FILES_INFO[CREATE_COORDINATION_NODE];
}

const TFileInfo& IncompleteData() {
    return FILES_INFO[INCOMPLETE_DATA];
}

const TFileInfo& Incomplete() {
    return FILES_INFO[INCOMPLETE];
}

const TFileInfo& Empty() {
    return FILES_INFO[EMPTY];
}

const TFileInfo& CreateView() {
    return FILES_INFO[CREATE_VIEW];
}

} // NYdb::NDump::NFiles

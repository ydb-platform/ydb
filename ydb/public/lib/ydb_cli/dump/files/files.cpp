#include "files.h"

namespace NYdb::NDump::NFiles {

enum EFilesType {
    SCHEME = 0,
    PERMISSIONS,
    CHANGEFEED_DESCRIPTION,
    TOPIC_DESCRIPTION,
    CREATE_TOPIC,
    CREATE_COORDINATION_NODE,
    CREATE_RATE_LIMITER,
    INCOMPLETE_DATA,
    INCOMPLETE,
    EMPTY,
    CREATE_VIEW,
    DATABASE,
    CREATE_USER,
    CREATE_GROUP,
    ALTER_GROUP,
    CREATE_ASYNC_REPLICATION,
    CREATE_EXTERNAL_DATA_SOURCE,
    CREATE_EXTERNAL_TABLE,
};

static constexpr TFileInfo FILES_INFO[] = {
    {"scheme.pb", "scheme"},
    {"permissions.pb", "ACL"},
    {"changefeed_description.pb", "changefeed"},
    {"topic_description.pb", "topic"},
    {"create_topic.pb", "topic"},
    {"create_coordination_node.pb", "coordination node"},
    {"create_rate_limiter.pb", "rate limiter"},
    {"incomplete.csv", "incomplete"},
    {"incomplete", "incomplete"},
    {"empty_dir", "empty_dir"},
    {"create_view.sql", "view"},
    {"database.pb", "database description"},
    {"create_user.sql", "users"},
    {"create_group.sql", "groups"},
    {"alter_group.sql", "group members"},
    {"create_async_replication.sql", "async replication"},
    {"create_external_data_source.sql", "external data source"},
    {"create_external_table.sql", "external table"},
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

const TFileInfo& CreateRateLimiter() {
    return FILES_INFO[CREATE_RATE_LIMITER];
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

const TFileInfo& Database() {
    return FILES_INFO[DATABASE];
}

const TFileInfo& CreateUser() {
    return FILES_INFO[CREATE_USER];
}

const TFileInfo& CreateGroup() {
    return FILES_INFO[CREATE_GROUP];
}

const TFileInfo& AlterGroup() {
    return FILES_INFO[ALTER_GROUP];
}

const TFileInfo& CreateAsyncReplication() {
    return FILES_INFO[CREATE_ASYNC_REPLICATION];
}

const TFileInfo& CreateExternalDataSource() {
    return FILES_INFO[CREATE_EXTERNAL_DATA_SOURCE];
}

const TFileInfo& CreateExternalTable() {
    return FILES_INFO[CREATE_EXTERNAL_TABLE];
}

} // NYdb::NDump::NFiles

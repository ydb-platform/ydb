#include "files.h"

#include <ydb/public/lib/ydb_cli/dump/util/util.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <util/generic/string.h>
#include <util/system/file.h>

namespace NYdb::NBackup::NFiles {

enum EFilesType {
    SCHEME = 0,
    PERMISSIONS,
    CHANGEFEED_DESCRIPTION,
    TOPIC_DESCRIPTION,
    INCOMPLETE_DATA,
    INCOMPLETE,
    EMPTY
};

static constexpr TFileInfo FILES_INFO[] = {
    {"scheme.pb", "scheme"},
    {"permissions.pb", "ACL"},
    {"changefeed_description.pb", "changefeed"},
    {"topic_description.pb", "topic"},
    {"incomplete.csv", "incomplete"},
    {"incomplete", "incomplete"},
    {"empty_dir", "empty_dir"}
};

const TFileInfo& TableScheme() {
    return FILES_INFO[SCHEME];
}

const TFileInfo& TablePermissions() {
    return FILES_INFO[PERMISSIONS];
}

const TFileInfo& Changefeed() {
    return FILES_INFO[CHANGEFEED_DESCRIPTION];
}

const TFileInfo& Topic() {
    return FILES_INFO[TOPIC_DESCRIPTION];
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

} // NYdb::NBackup::NFiles

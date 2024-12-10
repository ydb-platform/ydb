#pragma once

#include <contrib/libs/protobuf/src/google/protobuf/message.h>

#include <util/folder/path.h>

namespace NYdb::NBackup::NFiles {

struct TFileInfo {
    const char* FileName;
    const char* LogObjectType;
};

enum EFilesType {
    SCHEME = 0,
    PERMISSIONS,
    CHANGEFEED_DESCRIPTION,
    TOPIC_DESCRIPTION,
    INCOMPLETE_DATA,
    INCOMPLETE,
    EMPTY
};

void WriteProtoToFile(const google::protobuf::Message& proto, const TFsPath& folderPath, EFilesType type);

const TFileInfo& TableScheme();
const TFileInfo& TablePermissions();
const TFileInfo& Changefeed();
const TFileInfo& Topic();
const TFileInfo& IncompleteData();
const TFileInfo& Incomplete();
const TFileInfo& Empty();

} // NYdb::NBackup:NFiles

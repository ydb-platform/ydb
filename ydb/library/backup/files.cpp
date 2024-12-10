#include "files.h"
#include "util.h"

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <util/generic/string.h>
#include <util/system/file.h>

namespace NYdb::NBackup::NFiles {

static constexpr TFileInfo FILES_INFO[] = {
    {"scheme.pb", "scheme"},
    {"permissions.pb", "ACL"},
    {"changefeed_description.pb", "changefeed"},
    {"topic_description.pb", "topic"},
    {"incomplete.csv", "incomplete"},
    {"incomplete", "incomplete"},
    {"empty_dir", "empty_dir"}
};

void WriteProtoToFile(const google::protobuf::Message& proto, const TFsPath& folderPath, EFilesType type) {
    TString protoStr;
    google::protobuf::TextFormat::PrintToString(proto, &protoStr);
    const char* FILE_NAME = FILES_INFO[type].FileName;
    LOG_D("Write " << FILES_INFO[type].LogObjectType << " into " << folderPath.Child(FILE_NAME).GetPath().Quote());
    TFile outFile(folderPath.Child(FILE_NAME), CreateAlways | WrOnly);
    outFile.Write(protoStr.data(), protoStr.size());
}

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

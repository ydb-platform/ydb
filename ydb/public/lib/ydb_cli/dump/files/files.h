#pragma once

namespace NYdb::NBackup::NFiles {

struct TFileInfo {
    const char* FileName;
    const char* LogObjectType;
};

const TFileInfo& TableScheme();
const TFileInfo& Permissions();
const TFileInfo& Changefeed();
const TFileInfo& Topic();
const TFileInfo& IncompleteData();
const TFileInfo& Incomplete();
const TFileInfo& Empty();

} // NYdb::NBackup:NFiles

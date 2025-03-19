#pragma once

namespace NYdb::NDump::NFiles {

struct TFileInfo {
    const char* FileName;
    const char* LogObjectType;
};

const TFileInfo& TableScheme();
const TFileInfo& Permissions();
const TFileInfo& Changefeed();
const TFileInfo& TopicDescription();
const TFileInfo& CreateTopic();
const TFileInfo& CreateCoordinationNode();
const TFileInfo& CreateRateLimiter();
const TFileInfo& IncompleteData();
const TFileInfo& Incomplete();
const TFileInfo& Empty();
const TFileInfo& CreateView();
const TFileInfo& Database();
const TFileInfo& CreateUser();
const TFileInfo& CreateGroup();
const TFileInfo& AlterGroup();
const TFileInfo& CreateAsyncReplication();
const TFileInfo& CreateExternalDataSource();
const TFileInfo& CreateExternalTable();

} // NYdb::NDump:NFiles

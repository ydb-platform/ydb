#pragma once

#include <DBPoco/Timestamp.h>
#include <string>
#include <filesystem>
#include <Common/filesystemHelpers.h>


class FileUpdatesTracker
{
private:
    std::string path;
    DBPoco::Timestamp known_time;

public:
    explicit FileUpdatesTracker(const std::string & path_)
        : path(path_)
        , known_time(0)
    {}

    bool isModified() const
    {
        return getLastModificationTime() > known_time;
    }

    void fixCurrentVersion()
    {
        known_time = getLastModificationTime();
    }

private:
    DBPoco::Timestamp getLastModificationTime() const
    {
        return FS::getModificationTimestamp(path);
    }
};

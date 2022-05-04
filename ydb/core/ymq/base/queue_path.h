#pragma once
#include <util/generic/string.h>
#include <util/string/join.h>

namespace NKikimr::NSQS {

struct TQueuePath {
    TString Root;
    TString UserName;
    TString QueueName;
    ui64 Version = 0;
    TString VersionSuffix;

    TQueuePath()
    { }

    TQueuePath(const TString& root,
               const TString& userName,
               const TString& queueName,
               const ui64 version = 0)
        : Root(root)
        , UserName(userName)
        , QueueName(queueName)
        , Version(version)
    {
        if (version) {
            VersionSuffix = TString::Join("v", ToString(version));
        }
    }

    operator TString() const {
        return GetQueuePath();
    }

    TString GetQueuePath() const {
        return Join("/", Root, UserName, QueueName);
    }

    TString GetVersionedQueuePath() const {
        auto tmp = GetQueuePath();
        if (!VersionSuffix) {
            return tmp;
        } else {
            return Join("/", tmp, VersionSuffix);
        }
    }

    TString GetUserPath() const {
        return Join("/", Root, UserName);
    }

    TString GetRootPath() const {
        return Root;
    }
};

} // namespace NKikimr::NSQS

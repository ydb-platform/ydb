#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/guid.h>
#include <util/generic/maybe.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NYql {

class IFileCache: public TSimpleRefCount<IFileCache> {
public:
    using TPtr = TIntrusivePtr<IFileCache>;

    virtual ~IFileCache() = default;

    virtual void Clear() { }

    virtual void AddFile(const TString& path, const TString& objectId) = 0;

    virtual TMaybe<TString> FindFile(const TString& objectId) = 0;
    virtual TMaybe<TString> AcquireFile(const TString& objectId) = 0;
    virtual void ReleaseFile(const TString& objectId) = 0;

    virtual bool Contains(const TString& objectId) = 0;

    virtual void Walk(const std::function<void(const TString& objectId)>&) = 0;

    virtual i64 FreeDiskSize() = 0;

    virtual ui64 UsedDiskSize() = 0;

    virtual TString GetDir() = 0;
};

} // namespace NYql

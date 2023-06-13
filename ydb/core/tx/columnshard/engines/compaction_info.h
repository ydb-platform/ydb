#pragma once
#include <util/generic/string.h>
#include <util/system/yassert.h>
#include <util/stream/output.h>
#include <memory>

namespace NKikimr::NOlap {

class ICompactionObjectCallback {
public:
    virtual ~ICompactionObjectCallback() = default;
    virtual void OnCompactionStarted(const bool inGranule) = 0;
    virtual void OnCompactionFinished() = 0;
    virtual void OnCompactionFailed(const TString& reason) = 0;
    virtual void OnCompactionCanceled(const TString& reason) = 0;
    virtual TString DebugString() const = 0;
};

class TPlanCompactionInfo {
private:
    ui64 PathId = 0;
    bool InternalFlag = false;
public:
    TPlanCompactionInfo(const ui64 pathId, const bool internalFlag)
        : PathId(pathId)
        , InternalFlag(internalFlag) {

    }

    ui64 GetPathId() const {
        return PathId;
    }

    bool IsInternal() const {
        return InternalFlag;
    }
};

struct TCompactionInfo {
private:
    std::shared_ptr<ICompactionObjectCallback> CompactionObject;
    mutable bool StatusProvided = false;
    const bool InGranuleFlag = false;
public:
    TCompactionInfo(std::shared_ptr<ICompactionObjectCallback> compactionObject, const bool inGranule)
        : CompactionObject(compactionObject)
        , InGranuleFlag(inGranule)
    {
        Y_VERIFY(compactionObject);
        CompactionObject->OnCompactionStarted(InGranuleFlag);
    }

    TPlanCompactionInfo GetPlanCompaction() const;

    bool InGranule() const {
        return InGranuleFlag;
    }

    template <class T>
    const T& GetObject() const {
        auto result = dynamic_cast<const T*>(CompactionObject.get());
        Y_VERIFY(result);
        return *result;
    }

    void CompactionFinished() const {
        Y_VERIFY(!StatusProvided);
        StatusProvided = true;
        CompactionObject->OnCompactionFinished();
    }

    void CompactionCanceled(const TString& reason) const {
        Y_VERIFY(!StatusProvided);
        StatusProvided = true;
        CompactionObject->OnCompactionCanceled(reason);
    }

    void CompactionFailed(const TString& reason) const {
        Y_VERIFY(!StatusProvided);
        StatusProvided = true;
        CompactionObject->OnCompactionFailed(reason);
    }

    ~TCompactionInfo() {
        Y_VERIFY_DEBUG(StatusProvided);
        if (!StatusProvided) {
            CompactionObject->OnCompactionFailed("compaction unexpectedly finished");
        }
    }

    friend IOutputStream& operator << (IOutputStream& out, const TCompactionInfo& info) {
        out << (info.InGranuleFlag ? "in granule" : "split granule") << " compaction of granule: " << info.CompactionObject->DebugString();
        return out;
    }
};

}

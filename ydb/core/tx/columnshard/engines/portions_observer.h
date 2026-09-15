#pragma once

namespace NKikimr::NOlap {
class TPortionDataAccessor;
class TPortionInfo;

// Called in Complete, after the portion is visible or before it is gone.
class IPortionsObserver {
public:
    virtual ~IPortionsObserver() = default;
    virtual void OnPortionAdded(const TPortionDataAccessor& accessor) = 0;
    virtual void OnPortionErased(const TPortionInfo& portion) = 0;
};

}   // namespace NKikimr::NOlap

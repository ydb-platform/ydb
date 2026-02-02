#include "guarded_sglist.h"

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

class TGuardedSgList::TGuardedObject final: public IGuardedObject
{
private:
    // Refs==1 means that access is open, clients can get access by incrementing
    // the counter.
    // Refs==0 means that access to the object has been closed and
    // no one should increase the counter.
    // The transition from the value 1 to 0 is performed by the Close() method.
    TAtomic Refs = 1;

public:
    // Acquire can only be successed when Refs values are greater than zero
    // otherwise false will be returned.
    // This is non-blocking call.
    bool Acquire() override
    {
        while (true) {
            const intptr_t curValue = AtomicGet(Refs);

            if (!curValue) {
                return false;
            }

            if (AtomicCas(&Refs, curValue + 1, curValue)) {
                return true;
            }
        }
    }

    // Release calls must be paired with acquire calls.
    // This is non-blocking call.
    void Release() override
    {
        AtomicSub(Refs, 1);
    }

    // Close() waits until the Refs counter is equal to 1 and transfer it to 0
    // to prohibit further successful Acquire. This call may be blocked until
    // other threads call Release().
    void Close() override
    {
        while (true) {
            const intptr_t curValue = AtomicGet(Refs);

            if (!curValue) {
                return;
            }

            if (curValue != 1) {
                continue;
            }

            if (AtomicCas(&Refs, 0, 1)) {
                return;
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGuardedSgList::TDependentGuardedObject final: public IGuardedObject
{
private:
    TGuardedObject GuardedObject;
    const TIntrusivePtr<IGuardedObject> Dependee;

public:
    explicit TDependentGuardedObject(TIntrusivePtr<IGuardedObject> dependee)
        : Dependee(std::move(dependee))
    {}

    bool Acquire() override
    {
        if (GuardedObject.Acquire()) {
            if (Dependee->Acquire()) {
                return true;
            }

            GuardedObject.Release();
            GuardedObject.Close();
        }
        return false;
    }

    void Release() override
    {
        Dependee->Release();
        GuardedObject.Release();
    }

    void Close() override
    {
        GuardedObject.Close();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGuardedSgList::TUnionGuardedObject final: public IGuardedObject
{
private:
    TGuardedObject GuardedObject;
    const TVector<TGuardedSgList> GuardedSgLists;

public:
    explicit TUnionGuardedObject(TVector<TGuardedSgList> guardedSgLists)
        : GuardedSgLists(std::move(guardedSgLists))
    {}

    bool Acquire() override
    {
        if (GuardedObject.Acquire()) {
            if (AcquireObjects()) {
                return true;
            }

            GuardedObject.Release();
            GuardedObject.Close();
        }
        return false;
    }

    void Release() override
    {
        for (auto& guardedSgList: GuardedSgLists) {
            guardedSgList.GuardedObject->Release();
        }
        GuardedObject.Release();
    }

    void Close() override
    {
        GuardedObject.Close();
    }

private:
    bool AcquireObjects()
    {
        for (size_t i = 0; i < GuardedSgLists.size(); ++i) {
            auto& guardedSgList = GuardedSgLists[i];

            if (!guardedSgList.GuardedObject->Acquire()) {
                // release acquired objects
                for (size_t j = 0; j < i; ++j) {
                    GuardedSgLists[j].GuardedObject->Release();
                }
                return false;
            }
        }
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

TGuardedSgList::TGuard::TGuard(TIntrusivePtr<IGuardedObject> guardedObject,
                               const TSgList& sglist)
    : Sglist(sglist)
{
    if (guardedObject->Acquire()) {
        GuardedObject = std::move(guardedObject);
    }
}

TGuardedSgList::TGuard::~TGuard()
{
    if (GuardedObject) {
        GuardedObject->Release();
    }
}

TGuardedSgList::TGuard::operator bool() const
{
    return GuardedObject != nullptr;
}

const TSgList& TGuardedSgList::TGuard::Get() const
{
    Y_ABORT_UNLESS(GuardedObject);
    return Sglist;
}

////////////////////////////////////////////////////////////////////////////////

// static
TGuardedSgList TGuardedSgList::CreateUnion(
    TVector<TGuardedSgList> guardedSgLists)
{
    if (guardedSgLists.empty()) {
        return TGuardedSgList();
    }

    if (guardedSgLists.size() == 1) {
        return std::move(guardedSgLists.front());
    }

    TSgList joinedSgList;
    size_t cap = 0;
    for (const auto& guardedSgList: guardedSgLists) {
        cap += guardedSgList.Sglist.size();
    }
    joinedSgList.reserve(cap);

    for (const auto& guardedSgList: guardedSgLists) {
        const auto& sgList = guardedSgList.Sglist;
        joinedSgList.insert(joinedSgList.end(), sgList.begin(), sgList.end());
    }

    return TGuardedSgList(
        MakeIntrusive<TUnionGuardedObject>(std::move(guardedSgLists)),
        std::move(joinedSgList));
}

TGuardedSgList::TGuardedSgList()
    : GuardedObject(MakeIntrusive<TGuardedObject>())
{}

TGuardedSgList::TGuardedSgList(TSgList sglist)
    : GuardedObject(MakeIntrusive<TGuardedObject>())
    , Sglist(std::move(sglist))
{}

TGuardedSgList::TGuardedSgList(TIntrusivePtr<IGuardedObject> guardedObject,
                               TSgList sglist)
    : GuardedObject(std::move(guardedObject))
    , Sglist(std::move(sglist))
{}

bool TGuardedSgList::Empty() const
{
    return Sglist.empty();
}

TGuardedSgList TGuardedSgList::CreateDepender() const
{
    Y_ABORT_UNLESS(GuardedObject);
    return TGuardedSgList(MakeIntrusive<TDependentGuardedObject>(GuardedObject),
                          Sglist);
}

TGuardedSgList TGuardedSgList::CreateDepender(TSgList sglist) const
{
    Y_ABORT_UNLESS(GuardedObject);
    return TGuardedSgList(MakeIntrusive<TDependentGuardedObject>(GuardedObject),
                          std::move(sglist));
}

TGuardedSgList TGuardedSgList::Create(TSgList sglist) const
{
    Y_ABORT_UNLESS(GuardedObject);
    return TGuardedSgList(GuardedObject, std::move(sglist));
}

void TGuardedSgList::SetSgList(TSgList sglist)
{
    Y_ABORT_UNLESS(GuardedObject);
    Sglist = std::move(sglist);
}

TGuardedSgList::TGuard TGuardedSgList::Acquire() const
{
    Y_ABORT_UNLESS(GuardedObject);
    return TGuard(GuardedObject, Sglist);
}

void TGuardedSgList::Close()
{
    Y_ABORT_UNLESS(GuardedObject);
    GuardedObject->Close();
}

}   // namespace NYdb::NBS

#pragma once

#include "private.h"
#include "polymorphic.h"

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/id_generator.h>

namespace NYT::NPhoenix2 {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TSerializer;

class TContextBase
{ };

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NDetail::TContextBase
    , public TStreamSaveContext
{
public:
    explicit TSaveContext(
        IZeroCopyOutput* output,
        int version = 0);

private:
    friend struct NDetail::TSerializer;

    TIdGenerator ObjectIdGenerator_;

    struct TObjectEntry
    {
        TObjectId Id;
        std::optional<std::type_index> TypeIndex;
    };

    THashMap<void*, TObjectEntry> PtrToObjectEntry_;

    TObjectId GenerateObjectId(void* basePtr, std::optional<std::type_index> typeIndex);
    TObjectId FindObjectId(void* basePtr, std::optional<std::type_index> typeIndex) const;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NDetail::TContextBase
    , public TStreamLoadContext
{
public:
    using TStreamLoadContext::TStreamLoadContext;
    ~TLoadContext();

private:
    friend struct NDetail::TSerializer;

    THashMap<TObjectId, void*> IdToPtr_;
    std::vector<std::function<void()>> Deletors_;

    void RegisterObject(TObjectId id, void* basePtr);
    void* GetObject(TObjectId id) const;

    template <class T>
    void RegisterConstructedObject(T* ptr);
};

////////////////////////////////////////////////////////////////////////////////

template <class TSaveContext, class TLoadContext>
struct ICustomPersistent
    : public TPolymorphicBase
{
    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;
};

using IPersistent = ICustomPersistent<TSaveContext, TLoadContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix2

#define CONTEXT_INL_H_
#include "context-inl.h"
#undef CONTEXT_INL_H_

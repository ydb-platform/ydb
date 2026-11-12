#pragma once

#include "private.h"
#include "polymorphic.h"

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/id_generator.h>

namespace NYT::NPhoenix {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TSerializer;
struct TUniverseLoadSchedule;

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
    explicit TLoadContext(IInputStream* input);
    explicit TLoadContext(IZeroCopyInput* input);
    ~TLoadContext();

    //! Types whose schemas in #schema differ from native ones become compat-loaded.
    //! Must be called before loading any Phoenix types.
    void SetSchema(const TUniverseSchemaPtr& schema);

    //! Null unless some type needs compat loading.
    NDetail::TUniverseLoadSchedule* GetLoadSchedule();

private:
    friend struct NDetail::TSerializer;

    std::unique_ptr<NDetail::TUniverseLoadSchedule> LoadSchedule_;

    THashMap<TObjectId, void*> IdToPtr_;
    std::vector<std::function<void()>> Deleters_;

    void RegisterObject(TObjectId id, void* basePtr);
    void* GetObject(TObjectId id) const;
};

////////////////////////////////////////////////////////////////////////////////

template <class TSaveContext, class TLoadContext>
struct ICustomPersistent
    : public virtual TPolymorphicBase
{
    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;
};

using IPersistent = ICustomPersistent<TSaveContext, TLoadContext>;
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPhoenix

#define CONTEXT_INL_H_
#include "context-inl.h"
#undef CONTEXT_INL_H_

#ifndef INTERN_REGISTRY_INL_H_
#error "Direct inclusion of this file is not allowed, include intern_registry.h"
// For the sake of sane code completion.
#include "intern_registry.h"
#endif

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
template <class U>
TInternedObjectDataPtr<T> TInternRegistryTraits<T>::ConstructData(
    TInternRegistryPtr<T> registry,
    U&& data)
{
    return New<TInternedObjectData<T>>(
        std::forward<U>(data),
        std::move(registry));
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
template <class U>
TInternedObject<T> TInternRegistry<T>::Intern(U&& data)
{
    if (TInternRegistry<T>::GetDefault()->GetData() == data) {
        return TInternedObject<T>();
    }
    auto guard = Guard(Lock_);
    typename TProfilerSet::insert_ctx context;
    if (auto it = Registry_.find(data, context)) {
        return TInternedObject<T>(MakeStrong(*it));
    } else {
        auto internedData = TInternRegistryTraits<T>::ConstructData(this, std::forward<U>(data));
        it = Registry_.insert_direct(internedData.Get(), context);
        internedData->Iterator_ = it;
        return TInternedObject<T>(std::move(internedData));
    }
}

template <class T>
int TInternRegistry<T>::GetSize() const
{
    auto guard = Guard(Lock_);
    return static_cast<int>(Registry_.size());
}

template <class T>
TInternedObjectDataPtr<T> TInternRegistry<T>::GetDefault()
{
    static const auto Default = New<TInternedObjectData<T>>(T(), nullptr);
    return Default;
}

template <class T>
void TInternRegistry<T>::OnInternedDataDestroyed(TInternedObjectData<T>* data)
{
    auto guard = Guard(Lock_);
    Registry_.erase(data->Iterator_);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
size_t TInternRegistry<T>::THash::operator()(const TInternedObjectData<T>* internedData) const
{
    return internedData->GetHash();
}

template <class T>
size_t TInternRegistry<T>::THash::operator()(const T& data) const
{
    return ::THash<T>()(data);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TInternRegistry<T>::TEqual::operator()(
    const TInternedObjectData<T>* lhs,
    const TInternedObjectData<T>* rhs) const
{
    return lhs == rhs;
}

template <class T>
bool TInternRegistry<T>::TEqual::operator()(
    const TInternedObjectData<T>* lhs,
    const T& rhs) const
{
    return lhs->GetData() == rhs;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TInternedObjectData<T>::~TInternedObjectData()
{
    if (Registry_) {
        Registry_->OnInternedDataDestroyed(this);
    }
}

template <class T>
const T& TInternedObjectData<T>::GetData() const
{
    return Data_;
}

template <class T>
size_t TInternedObjectData<T>::GetHash() const
{
    return Hash_;
}

template <class T>
TInternedObjectData<T>::TInternedObjectData(T&& data, TInternRegistryPtr<T> registry)
    : Data_(std::move(data))
    , Hash_(THash<T>()(Data_))
    , Registry_(std::move(registry))
{ }

template <class T>
TInternedObjectData<T>::TInternedObjectData(const T& data, TInternRegistryPtr<T> registry)
    : Data_(data)
    , Hash_(THash<T>()(Data_))
    , Registry_(std::move(registry))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
TInternedObject<T>::TInternedObject()
    : Data_(TInternRegistry<T>::GetDefault())
{ }

template <class T>
TInternedObject<T>::operator bool() const
{
    return Data_.operator bool();
}

template <class T>
const T& TInternedObject<T>::operator*() const
{
    return Data_->GetData();
}

template <class T>
const T* TInternedObject<T>::operator->() const
{
    return &Data_->GetData();
}

template <class T>
TInternedObjectDataPtr<T> TInternedObject<T>::ToDataPtr() const
{
    return Data_;
}

template <class T>
TInternedObject<T> TInternedObject<T>::FromDataPtr(TInternedObjectDataPtr<T> data)
{
    return TInternedObject<T>(std::move(data));
}

template<class T>
bool TInternedObject<T>::RefEqual(const TInternedObject <T>& lhs, const TInternedObject <T>& rhs)
{
    return lhs.Data_ == rhs.Data_;
}

template <class T>
TInternedObject<T>::TInternedObject(TInternedObjectDataPtr<T> data)
    : Data_(std::move(data))
{ }

////////////////////////////////////////////////////////////////////////////////

struct TInternedObjectSerializer
{
    template <class C, class T>
    static void Save(C& context, const TInternedObject<T>& object)
    {
        using NYT::Save;

        auto key = context.RegisterRefCountedEntity(object.ToDataPtr());
        Save(context, key);
        if (key == TEntityStreamSaveContext::InlineKey) {
            Save(context, *object);
        }
    }

    template <class C, class T>
    static void Load(C& context, TInternedObject<T>& object)
    {
        using NYT::Load;

        auto key = NYT::LoadSuspended<TEntitySerializationKey>(context);
        if (key == TEntityStreamSaveContext::InlineKey) {
            SERIALIZATION_DUMP_INDENT(context) {
                auto value = Load<T>(context);
                const auto& registry = context.template GetInternRegistry<T>();
                object = registry->Intern(std::move(value));
                auto loadedKey = context.RegisterRefCountedEntity(object.ToDataPtr());
                SERIALIZATION_DUMP_WRITE(context, "objref %v", loadedKey);
            }
        } else {
            object = TInternedObject<T>::FromDataPtr(context.template GetRefCountedEntity<TInternedObjectData<T>>(key));
            SERIALIZATION_DUMP_WRITE(context, "objref %v", key);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
struct TSerializerTraits<
    TInternedObject<T>,
    C,
    void
>
{
    using TSerializer = TInternedObjectSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

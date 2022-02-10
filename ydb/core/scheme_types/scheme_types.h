#pragma once

#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <util/string/hex.h>
#include <util/string/cast.h>

#include <typeinfo>


namespace NKikimr {
namespace NScheme {

////////////////////////////////////////////////////////
class ITypeMetadata {
public:
    enum class EFlags {
        CanBeValueInKey = 0x01,
        CanCompare = 0x02,
        CanEquate = 0x04,
        CanHash = 0x08,
        HasDeterministicCompare = 0x10,
        HasDeterministicEquals = 0x20,
        HasDeterministicToString = 0x40,
        HasDeterministicHash = 0x80,
        HasDeterministicBytes = 0x100,
    };

    virtual ~ITypeMetadata() {}

    virtual TTypeId GetTypeId() const = 0;
    virtual const char* GetName() const = 0;
};

class IType : public ITypeMetadata {
friend class ITypeSP;
friend class TTypeRegistry;
};

////////////////////////////////////////////////////////
class ITypeSP {
public:
    //
    ITypeSP(const IType* t = nullptr)
        : Type(t)
        , TypeId(t ? t->GetTypeId() : 0)
    {}

    ITypeSP(TTypeId typeId)
        : Type(nullptr)
        , TypeId(typeId)
    {}

    //
    const IType* operator->() const noexcept { return Type; }
    explicit operator bool() const noexcept { return TypeId != 0; }

    bool IsKnownType() const noexcept { return Type != nullptr; }

    TTypeId GetTypeId() const noexcept { return TypeId; }
    const IType* GetType() const noexcept { return Type; }

private:
    const IType* Type;
    TTypeId TypeId;
};

} // namspace NScheme
} // namespace NKikimr

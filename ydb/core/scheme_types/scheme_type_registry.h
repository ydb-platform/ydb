#pragma once

#include "scheme_types.h"
#include "scheme_type_metadata.h"

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>


namespace NKikimr {
namespace NScheme {

////////////////////////////////////////////////////////
/// The TTypeRegistry class
/// this class is _not_ threadsafe!!!!
/// it's intentionally
class TTypeRegistry : public TThrRefBase, TNonCopyable {
public:
    //
    TTypeRegistry();

    //
    template <typename T>
    void RegisterType() {
        RegisterType(Singleton<T>());
    }

    void RegisterType(const IType *type) {
        const TTypeId typeId = type->GetTypeId();
        Y_ENSURE(typeId <= Max<TTypeId>());

        Y_ENSURE(TypeByIdMap.insert({ typeId, type }).second);
        Y_ENSURE(TypeByNameMap.insert({ type->GetName(), type }).second);

        TypeMetadataRegistry.Register(type);
    }

    //
    ITypeSP GetType(TTypeId typeId) const {
        if (typeId) {
            auto iter = TypeByIdMap.find(typeId);
            if (iter != TypeByIdMap.end()) {
                Y_ENSURE(iter->second);
                return iter->second;
            }
        }
        return typeId;
    }

    ::TString GetTypeName(TTypeId typeId) const {
        if (!typeId) {
            return "Null";
        }
        auto type = GetType(typeId);
        return type.IsKnownType() ? ::TString(type->GetName()) : (TStringBuilder() << "Unknown(" << typeId << ")");
    }

    ITypeSP GetKnownType(TTypeId typeId) const {
        if (!typeId)
            ythrow yexception() << "Type id must be non zero";

        auto type = GetType(typeId);
        if (Y_LIKELY(type))
            return type;
        ythrow yexception() << "Unknown type: " << typeId;
    }

    const IType* GetType(const TStringBuf& name) const {
        auto iter = TypeByNameMap.find(name);
        return iter != TypeByNameMap.end() ? iter->second : nullptr;
    }

    const IType* GetKnownType(const TStringBuf& name) const {
        auto type = GetType(name);
        if (Y_LIKELY(type))
            return type;
        ythrow yexception() << "Unknown type: " << name;
    }

    TVector<const IType*> GetTypes() const {
        TVector<const IType*> types;
        types.reserve(TypeByIdMap.size());
        for (const auto& entry : TypeByIdMap)
            types.push_back(entry.second);
        return types;
    }

    TTypeMetadataRegistry& GetTypeMetadataRegistry() {
        return TypeMetadataRegistry;
    }

    const TTypeMetadataRegistry& GetTypeMetadataRegistry() const {
        return TypeMetadataRegistry;
    }

    void CalculateMetadataEtag();

    ui64 GetMetadataEtag() const {
        return MetadataEtag;
    }

private:
    //
    typedef TMap<ui32, const IType *> TTypeByIdMap;
    typedef TMap<::TString, const IType *> TTypeByNameMap;

    TTypeByIdMap TypeByIdMap;
    TTypeByNameMap TypeByNameMap;
    TTypeMetadataRegistry TypeMetadataRegistry;
    ui64 MetadataEtag = 0;
};

} // namespace NScheme
} // namespace NKikimr


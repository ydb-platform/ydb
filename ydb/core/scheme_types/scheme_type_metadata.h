#pragma once

#include "scheme_raw_type_value.h"
#include "scheme_types.h"

#include <util/generic/hash.h>
#include <util/generic/yexception.h>


namespace NKikimr {
namespace NScheme {

class TTypeMetadataRegistry: private TNonCopyable {
public:
    struct TTypeMetadata: public ITypeMetadata {
    public:
        TTypeMetadata()
            : TypeId(0)
        {}

        TTypeMetadata(const TTypeId typeId, const ::TString& name)
            : TypeId(typeId)
            , Name(name)
        {}

        TTypeId GetTypeId() const override {
            return TypeId;
        }

        const char* GetName() const override {
            return Name.data();
        }

    private:
        TTypeId TypeId;
        ::TString Name;
    };

    typedef THashMap<TTypeId, const ITypeMetadata*> TMapById;
    typedef THashMap<::TString, const ITypeMetadata*> TMapByName;

    void Register(const ITypeMetadata* metadata) {
        Y_ENSURE(MapById.insert({ metadata->GetTypeId(), metadata }).second);
        Y_ENSURE(MapByName.insert({ metadata->GetName(), metadata }).second);
    }

    const ITypeMetadata* GetType(TTypeId typeId) const {
        auto it = MapById.find(typeId);
        if (it == MapById.end())
            return nullptr;
        return it->second;
    }

    const ITypeMetadata* GetKnownType(TTypeId typeId) const {
        if (!typeId)
            ythrow yexception() << "Type id must be non zero";

        auto typeMetadata = GetType(typeId);
        if (typeMetadata) return typeMetadata;

        ythrow yexception() << "Unknown type: " << typeId;
    }

    const ITypeMetadata* GetType(const TStringBuf& name) const {
        auto it = MapByName.find(name);
        if (it == MapByName.end())
            return nullptr;
        return it->second;
    }

    TMapById::const_iterator begin() const {
        return MapById.begin();
    }

    TMapById::const_iterator end() const {
        return MapById.end();
    }

    void Clear() {
        MapById.clear();
        MapByName.clear();
    }

private:
    TMapById MapById;
    TMapByName MapByName;
};

} // namespace NScheme
} // namespace NKikimr

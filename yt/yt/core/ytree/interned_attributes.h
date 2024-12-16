#pragma once

#include "public.h"

#include <library/cpp/yt/misc/preprocessor.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TInternedAttributeKey
{
public:
    TInternedAttributeKey();

    explicit constexpr TInternedAttributeKey(size_t code)
        : Code_(code)
    { }

    constexpr operator size_t() const;

    // May return #InvalidInternedAttribute if the attribute is not interned.
    static TInternedAttributeKey Lookup(TStringBuf uninternedKey);

    const TString& Unintern() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    // NB: this codes are subject to change! Do not rely on their values. Do not serialize them.
    // Use Save/Load methods instead.
    size_t Code_;
};

constexpr TInternedAttributeKey InvalidInternedAttribute{0};
constexpr TInternedAttributeKey CountInternedAttribute{1};

//! Interned attribute registry initialization. Should be called once per attribute.
//! Both interned and uninterned keys must be unique.
void InternAttribute(const TString& uninternedKey, TInternedAttributeKey internedKey);

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_INTERNED_ATTRIBUTE(uninternedKey, internedKey) \
    YT_ATTRIBUTE_USED const void* PP_ANONYMOUS_VARIABLE(RegisterInterndAttribute) = [] { \
            ::NYT::NYTree::InternAttribute(#uninternedKey, internedKey); \
            return nullptr; \
        } ();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define INTERNED_ATTRIBUTES_INL_H_
#include "interned_attributes-inl.h"
#undef INTERNED_ATTRIBUTES_INL_H_

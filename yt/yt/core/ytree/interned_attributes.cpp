#include "interned_attributes.h"

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/serialize.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TInternedAttributeRegistry
{
public:
    void Intern(const TString& uninternedKey, TInternedAttributeKey internedKey)
    {
        YT_VERIFY(AttributeNameToIndex_.emplace(uninternedKey, internedKey).second);
        YT_VERIFY(AttributeIndexToName_.emplace(internedKey, uninternedKey).second);
    }

    TInternedAttributeKey GetInterned(TStringBuf uninternedKey)
    {
        auto it = AttributeNameToIndex_.find(uninternedKey);
        return it == AttributeNameToIndex_.end() ? InvalidInternedAttribute : it->second;
    }

    const TString& GetUninterned(TInternedAttributeKey internedKey)
    {
        return GetOrCrash(AttributeIndexToName_, internedKey);
    }

private:
    THashMap<TString, TInternedAttributeKey> AttributeNameToIndex_;
    THashMap<TInternedAttributeKey, TString> AttributeIndexToName_;
};

} // namespace

void InternAttribute(const TString& uninternedKey, TInternedAttributeKey internedKey)
{
    Singleton<TInternedAttributeRegistry>()->Intern(uninternedKey, internedKey);
}

////////////////////////////////////////////////////////////////////////////////

TInternedAttributeKey::TInternedAttributeKey()
    : Code_(InvalidInternedAttribute.Code_)
{ }

void TInternedAttributeKey::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, Unintern());
}

void TInternedAttributeKey::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    auto uninternedKey = Load<TString>(context);
    Code_ = Lookup(uninternedKey).Code_;
}

/*static*/ TInternedAttributeKey TInternedAttributeKey::Lookup(TStringBuf uninternedKey)
{
    return Singleton<TInternedAttributeRegistry>()->GetInterned(uninternedKey);
}

const TString& TInternedAttributeKey::Unintern() const
{
    return Singleton<TInternedAttributeRegistry>()->GetUninterned(*this);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_INTERNED_ATTRIBUTE(count, CountInternedAttribute)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

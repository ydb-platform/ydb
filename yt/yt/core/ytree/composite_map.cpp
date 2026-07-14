#include "composite_map.h"

#include "convert.h"
#include "virtual.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NYTree {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TCompositeMapService
    : public TVirtualMapBase
    , public ICompositeMapService
{
public:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        std::vector<std::string> keys;
        for (const auto& [key, _] : Services_) {
            if (std::ssize(keys) >= limit) {
                return keys;
            }
            keys.push_back(key);
        }
        for (const auto& producer : ChildrenProducers_) {
            for (const auto& key : ConvertToAttributes(producer)->ListKeys()) {
                if (std::ssize(keys) >= limit) {
                    return keys;
                }
                keys.push_back(key);
            }
        }
        return keys;
    }

    i64 GetSize() const override
    {
        i64 size = std::ssize(Services_);
        for (const auto& producer : ChildrenProducers_) {
            size += std::ssize(ConvertToAttributes(producer)->ListKeys());
        }
        return size;
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        if (auto it = Services_.find(key); it != Services_.end()) {
            return it->second;
        }
        for (const auto& producer : ChildrenProducers_) {
            if (auto value = ConvertToAttributes(producer)->FindYson(key)) {
                return ConvertToNode(value);
            }
        }
        return nullptr;
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        for (const auto& [key, _] : Attributes_) {
            descriptors->push_back(TAttributeDescriptor(key));
        }
        TVirtualMapBase::ListSystemAttributes(descriptors);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        if (auto it = Attributes_.find(key); it != Attributes_.end()) {
            it->second(consumer);
            return true;
        }
        return TVirtualMapBase::GetBuiltinAttribute(key, consumer);
    }

    TIntrusivePtr<ICompositeMapService> AddChild(const std::string& key, IYPathServicePtr service) override
    {
        EmplaceOrCrash(Services_, key, std::move(service));
        return this;
    }

    TIntrusivePtr<ICompositeMapService> AddChildren(TYsonProducer producer) override
    {
        ChildrenProducers_.push_back(std::move(producer));
        return this;
    }

    TIntrusivePtr<ICompositeMapService> AddAttribute(TInternedAttributeKey key, TYsonCallback producer) override
    {
        EmplaceOrCrash(Attributes_, key, std::move(producer));
        return this;
    }

    TIntrusivePtr<ICompositeMapService> SetOpaque(bool opaque) override
    {
        TVirtualMapBase::SetOpaque(opaque);
        return this;
    }

private:
    THashMap<std::string, IYPathServicePtr> Services_;
    std::vector<TYsonProducer> ChildrenProducers_;
    THashMap<TInternedAttributeKey, TYsonCallback> Attributes_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

ICompositeMapServicePtr CreateCompositeMapService()
{
    return New<TCompositeMapService>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#include "mkql_linear.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql::NUdf;

namespace {

struct TMutDictSupport {
    TMutDictSupport(TType* keyType, NUdf::TStringRef tag)
        : Hash(MakeHashImpl(keyType))
        , Equate(MakeEquateImpl(keyType))
        , Tag(tag)
    {}

    const IHash::TPtr Hash;
    const IEquate::TPtr Equate;
    const NUdf::TStringRef Tag;
};

NUdf::TStringRef GetMutDictTag(TType* type) {
    return AS_TYPE(TResourceType, AS_TYPE(TLinearType, type)->GetItemType())->GetTag();
}

struct TGenericHash {
    const IHash* Hash;

    std::size_t operator()(const NUdf::TUnboxedValuePod& value) const {
        return Hash->Hash(value);
    }
};

struct TGenericEquals {
    const IEquate* Equate;

    bool operator()(
        const NUdf::TUnboxedValuePod& left,
        const NUdf::TUnboxedValuePod& right) const
    {
        return Equate->Equals(left, right);
    }
};

using TMutDictMap = std::unordered_map<
    NUdf::TUnboxedValue,
    NUdf::TUnboxedValue,
    TGenericHash,
    TGenericEquals,
    TMKQLAllocator<std::pair<const NUdf::TUnboxedValue, NUdf::TUnboxedValue>>>;

class TMutDictResource : public TComputationValue<TMutDictResource> {
public:
    TMutDictResource(TMemoryUsageInfo* memInfo, const TMutDictSupport& support)
        : TComputationValue(memInfo)
        , Tag_(support.Tag)
        , Map_(0, TGenericHash{support.Hash.Get()}, TGenericEquals{support.Equate.Get()})
    {
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return Tag_;
    }

    void* GetResource() override {
        return &Map_;
    }

    const NUdf::TStringRef Tag_;
    TMutDictMap Map_;
};

class TToMutDictWrapper : public TMutableComputationNode<TToMutDictWrapper> {
    using TSelf = TToMutDictWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TToMutDictWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag, IComputationNode* source, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , DependentNodes_(dependentNodes)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        auto res = ctx.HolderFactory.Create<TMutDictResource>(Support_);
        auto& map = *static_cast<TMutDictMap*>(res.GetResource());
        NUdf::TUnboxedValue key, value;
        for (auto it = input.GetDictIterator(); it.NextPair(key, value);) {
            map.emplace(key, value);
        }

        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TToMutDictWrapper::DependsOn, this, std::placeholders::_1));
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    const TComputationNodePtrVector DependentNodes_;
};

class TMutDictCreateWrapper : public TMutableComputationNode<TMutDictCreateWrapper> {
    using TSelf = TMutDictCreateWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictCreateWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , DependentNodes_(dependentNodes)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TMutDictResource>(Support_);
    }

private:
    void RegisterDependencies() const final {
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TMutDictCreateWrapper::DependsOn, this, std::placeholders::_1));
    }

    TMutDictSupport Support_;
    const TComputationNodePtrVector DependentNodes_;
};

class TFromMutDictWrapper : public TMutableComputationNode<TFromMutDictWrapper> {
    using TSelf = TFromMutDictWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    template <bool NoSwap>
    class TIterator : public TComputationValue<TIterator<NoSwap>> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, const TMutDictMap& map)
            : TComputationValue<TIterator<NoSwap>>(memInfo)
            , Map_(map)
            , Iterator_(Map_.begin())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (Iterator_ == Map_.end())
                    return false;
                ++Iterator_;
            }

            return Iterator_ != Map_.end();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip())
                return false;
            key = NoSwap ? Iterator_->first : Iterator_->second;
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key))
                return false;
            payload = NoSwap ? Iterator_->second : Iterator_->first;
            return true;
        }

        const TMutDictMap& Map_;
        typename TMutDictMap::const_iterator Iterator_;
        bool AtStart_;
    };

    class TValue : public TComputationValue<TValue> {
    public:
        TValue(TMemoryUsageInfo* memInfo, TMutDictMap&& map)
            : TComputationValue(memInfo)
            , Map_(std::move(map))
        {
        }

        bool Contains(const NUdf::TUnboxedValuePod& key) const override {
            return Map_.contains(key);
        }

        NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
            auto it = Map_.find(key);
            if (it == Map_.end()) {
                return {};
            }

            return it->second.MakeOptional();
        }

        NUdf::TUnboxedValue GetKeysIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator<true>(GetMemInfo(), Map_));
        }

        NUdf::TUnboxedValue GetDictIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator<true>(GetMemInfo(), Map_));
        }

        NUdf::TUnboxedValue GetPayloadsIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator<false>(GetMemInfo(), Map_));
        }

        ui64 GetDictLength() const override {
            return Map_.size();
        }

        bool HasDictItems() const override {
            return !Map_.empty();
        }

    private:
        const TMutDictMap Map_;
    };

    TFromMutDictWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag, IComputationNode* source)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        return ctx.HolderFactory.Create<TValue>(std::move(map));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

class TMutDictInsertWrapper : public TMutableComputationNode<TMutDictInsertWrapper> {
    using TSelf = TMutDictInsertWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictInsertWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source, IComputationNode* keySource, IComputationNode* payloadSource)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , KeySource_(keySource)
        , PayloadSource_(payloadSource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        auto [it, inserted] = map.emplace(key, NUdf::TUnboxedValue::Invalid());
        if (inserted) {
            auto payload = PayloadSource_->GetValue(ctx);
            it->second = payload;
        }

        return input.Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        this->DependsOn(KeySource_);
        this->DependsOn(PayloadSource_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    IComputationNode* const KeySource_;
    IComputationNode* const PayloadSource_;
};

class TMutDictUpsertWrapper : public TMutableComputationNode<TMutDictUpsertWrapper> {
    using TSelf = TMutDictUpsertWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictUpsertWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source, IComputationNode* keySource, IComputationNode* payloadSource)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , KeySource_(keySource)
        , PayloadSource_(payloadSource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        auto payload = PayloadSource_->GetValue(ctx);
        map[key] = payload;
        return input.Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        this->DependsOn(KeySource_);
        this->DependsOn(PayloadSource_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    IComputationNode* const KeySource_;
    IComputationNode* const PayloadSource_;
};

class TMutDictUpdateWrapper : public TMutableComputationNode<TMutDictUpdateWrapper> {
    using TSelf = TMutDictUpdateWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictUpdateWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source, IComputationNode* keySource, IComputationNode* payloadSource)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , KeySource_(keySource)
        , PayloadSource_(payloadSource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        auto it = map.find(key);
        if (it != map.cend()) {
            auto payload = PayloadSource_->GetValue(ctx);
            it->second = payload;
        }

        return input.Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        this->DependsOn(KeySource_);
        this->DependsOn(PayloadSource_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    IComputationNode* const KeySource_;
    IComputationNode* const PayloadSource_;
};

class TMutDictRemoveWrapper : public TMutableComputationNode<TMutDictRemoveWrapper> {
    using TSelf = TMutDictRemoveWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictRemoveWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source, IComputationNode* keySource)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , KeySource_(keySource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        map.erase(key);
        return input.Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        this->DependsOn(KeySource_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    IComputationNode* const KeySource_;
};

class TMutDictPopWrapper : public TMutableComputationNode<TMutDictPopWrapper> {
    using TSelf = TMutDictPopWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictPopWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source, IComputationNode* keySource)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , KeySource_(keySource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        auto it = map.find(key);
        if (it != map.cend()) {
            items[1] = it->second.MakeOptional();
            map.erase(it);
        }

        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        this->DependsOn(KeySource_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    IComputationNode* const KeySource_;
};

class TMutDictContainsWrapper : public TMutableComputationNode<TMutDictContainsWrapper> {
    using TSelf = TMutDictContainsWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictContainsWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source, IComputationNode* keySource)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , KeySource_(keySource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        items[1] = NUdf::TUnboxedValuePod(map.contains(key));
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        this->DependsOn(KeySource_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    IComputationNode* const KeySource_;
};

class TMutDictLookupWrapper : public TMutableComputationNode<TMutDictLookupWrapper> {
    using TSelf = TMutDictLookupWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictLookupWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source, IComputationNode* keySource)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
        , KeySource_(keySource)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        auto it = map.find(key);
        if (it != map.cend()) {
            items[1] = it->second.MakeOptional();
        }

        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
        this->DependsOn(KeySource_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
    IComputationNode* const KeySource_;
};

class TMutDictHasItemsWrapper : public TMutableComputationNode<TMutDictHasItemsWrapper> {
    using TSelf = TMutDictHasItemsWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictHasItemsWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        items[1] = NUdf::TUnboxedValuePod(!map.empty());
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

class TMutDictLengthWrapper : public TMutableComputationNode<TMutDictLengthWrapper> {
    using TSelf = TMutDictLengthWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictLengthWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        items[1] = NUdf::TUnboxedValuePod(ui64(map.size()));
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

class TMutDictItemsWrapper : public TMutableComputationNode<TMutDictItemsWrapper> {
    using TSelf = TMutDictItemsWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictItemsWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue* listItems;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        auto list = ctx.HolderFactory.CreateDirectArrayHolder(map.size(), listItems);
        ui64 index = 0;
        for (auto it = map.cbegin(); it != map.cend(); ++it, ++index) {
            NUdf::TUnboxedValue* pairItems;
            auto pair = ctx.HolderFactory.CreateDirectArrayHolder(2, pairItems);
            pairItems[0] = it->first;
            pairItems[1] = it->second;
            listItems[index] = pair;
        }

        items[0] = input;
        items[1] = list;
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

class TMutDictKeysWrapper : public TMutableComputationNode<TMutDictKeysWrapper> {
    using TSelf = TMutDictKeysWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictKeysWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue* listItems;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        auto list = ctx.HolderFactory.CreateDirectArrayHolder(map.size(), listItems);
        ui64 index = 0;
        for (auto it = map.cbegin(); it != map.cend(); ++it, ++index) {
            listItems[index] = it->first;
        }

        items[0] = input;
        items[1] = list;
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

class TMutDictPayloadsWrapper : public TMutableComputationNode<TMutDictPayloadsWrapper> {
    using TSelf = TMutDictPayloadsWrapper;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;
public:
    TMutDictPayloadsWrapper(TComputationMutables& mutables, TType* keyType, NUdf::TStringRef tag,
        IComputationNode* source)
        : TBaseComputation(mutables)
        , Support_(keyType, tag)
        , Source_(source)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto input = Source_->GetValue(ctx);
        Y_DEBUG_ABORT_UNLESS(input.GetResourceTag() == Support_.Tag);
        auto& map = *static_cast<TMutDictMap*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue* listItems;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        auto list = ctx.HolderFactory.CreateDirectArrayHolder(map.size(), listItems);
        ui64 index = 0;
        for (auto it = map.cbegin(); it != map.cend(); ++it, ++index) {
            listItems[index] = it->second;
        }

        items[0] = input;
        items[1] = list;
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

} // namespace

IComputationNode* WrapToMutDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1U, "Expected at least 1 arg");
    auto keyType = AS_TYPE(TDictType, callable.GetInput(0).GetStaticType())->GetKeyType();
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - 1);
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - 1] = LocateNode(ctx.NodeLocator, callable, i);
    }

    auto source = LocateNode(ctx.NodeLocator, callable, 0);
    auto tag = GetMutDictTag(callable.GetType()->GetReturnType());
    return new TToMutDictWrapper(ctx.Mutables, keyType, tag, source, std::move(dependentNodes));
}

IComputationNode* WrapMutDictCreate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1U, "Expected at least 1 arg");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - 1);
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - 1] = LocateNode(ctx.NodeLocator, callable, i);
    }

    auto tag = GetMutDictTag(callable.GetType()->GetReturnType());
    return new TMutDictCreateWrapper(ctx.Mutables, keyType, tag, std::move(dependentNodes));
}

IComputationNode* WrapMutDictInsert(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4U, "Expected 4 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto payloadSource = LocateNode(ctx.NodeLocator, callable, 3);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictInsertWrapper(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
}

IComputationNode* WrapMutDictUpsert(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4U, "Expected 4 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto payloadSource = LocateNode(ctx.NodeLocator, callable, 3);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictUpsertWrapper(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
}

IComputationNode* WrapMutDictUpdate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4U, "Expected 4 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto payloadSource = LocateNode(ctx.NodeLocator, callable, 3);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictUpdateWrapper(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
}

IComputationNode* WrapMutDictRemove(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictRemoveWrapper(ctx.Mutables, keyType, tag, source, keySource);
}

IComputationNode* WrapMutDictPop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictPopWrapper(ctx.Mutables, keyType, tag, source, keySource);
}

IComputationNode* WrapMutDictContains(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictContainsWrapper(ctx.Mutables, keyType, tag, source, keySource);
}

IComputationNode* WrapMutDictLookup(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictLookupWrapper(ctx.Mutables, keyType, tag, source, keySource);
}

IComputationNode* WrapMutDictLength(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictLengthWrapper(ctx.Mutables, keyType, tag, source);
}

IComputationNode* WrapMutDictHasItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictHasItemsWrapper(ctx.Mutables, keyType, tag, source);
}

IComputationNode* WrapMutDictItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictItemsWrapper(ctx.Mutables, keyType, tag, source);
}

IComputationNode* WrapMutDictKeys(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictKeysWrapper(ctx.Mutables, keyType, tag, source);
}

IComputationNode* WrapMutDictPayloads(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    return new TMutDictPayloadsWrapper(ctx.Mutables, keyType, tag, source);
}

IComputationNode* WrapFromMutDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1U, "Expected 1 arg");
    auto keyType = AS_TYPE(TDictType, callable.GetType()->GetReturnType());
    auto source = LocateNode(ctx.NodeLocator, callable, 0);
    auto tag = GetMutDictTag(callable.GetInput(0).GetStaticType());
    return new TFromMutDictWrapper(ctx.Mutables, keyType, tag, source);
}

}
}

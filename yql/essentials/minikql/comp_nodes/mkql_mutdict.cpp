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
    {
    }

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
        const NUdf::TUnboxedValuePod& right) const {
        return Equate->Equals(left, right);
    }
};

using TMutDictMap = std::unordered_map<
    NUdf::TUnboxedValue,
    NUdf::TUnboxedValue,
    TGenericHash,
    TGenericEquals,
    TMKQLAllocator<std::pair<const NUdf::TUnboxedValue, NUdf::TUnboxedValue>>>;

using TMutDictSet = std::unordered_set<
    NUdf::TUnboxedValue,
    TGenericHash,
    TGenericEquals,
    TMKQLAllocator<NUdf::TUnboxedValue>>;

template <bool IsSet>
using TMutDictStorage = std::conditional_t<IsSet, TMutDictSet, TMutDictMap>;

template <bool IsSet>
class TMutDictResource: public TComputationValue<TMutDictResource<IsSet>> {
    using TSelf = TMutDictResource<IsSet>;
    using TBase = TComputationValue<TSelf>;

public:
    TMutDictResource(TMemoryUsageInfo* memInfo, const TMutDictSupport& support)
        : TBase(memInfo)
        , Tag_(support.Tag)
        , Storage_(0, TGenericHash{support.Hash.Get()}, TGenericEquals{support.Equate.Get()})
    {
    }

private:
    NUdf::TStringRef GetResourceTag() const override {
        return Tag_;
    }

    void* GetResource() override {
        return &Storage_;
    }

    const NUdf::TStringRef Tag_;
    TMutDictStorage<IsSet> Storage_;
};

template <bool IsSet>
class TToMutDictWrapper: public TMutableComputationNode<TToMutDictWrapper<IsSet>> {
    using TSelf = TToMutDictWrapper<IsSet>;
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
        auto res = ctx.HolderFactory.Create<TMutDictResource<IsSet>>(Support_);
        if constexpr (IsSet) {
            auto& set = *static_cast<TMutDictSet*>(res.GetResource());
            NUdf::TUnboxedValue key;
            for (auto it = input.GetKeysIterator(); it.Next(key);) {
                set.emplace(key);
            }
        } else {
            auto& map = *static_cast<TMutDictMap*>(res.GetResource());
            NUdf::TUnboxedValue key, value;
            for (auto it = input.GetDictIterator(); it.NextPair(key, value);) {
                map.emplace(key, value);
            }
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

template <bool IsSet>
class TMutDictCreateWrapper: public TMutableComputationNode<TMutDictCreateWrapper<IsSet>> {
    using TSelf = TMutDictCreateWrapper<IsSet>;
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
        return ctx.HolderFactory.Create<TMutDictResource<IsSet>>(Support_);
    }

private:
    void RegisterDependencies() const final {
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TMutDictCreateWrapper::DependsOn, this, std::placeholders::_1));
    }

    TMutDictSupport Support_;
    const TComputationNodePtrVector DependentNodes_;
};

template <bool IsSet>
class TFromMutDictWrapper: public TMutableComputationNode<TFromMutDictWrapper<IsSet>> {
    using TSelf = TFromMutDictWrapper<IsSet>;
    using TBase = TMutableComputationNode<TSelf>;
    typedef TBase TBaseComputation;

public:
    using TStorage = TMutDictStorage<IsSet>;

    template <bool NoSwap>
    class TIterator: public TComputationValue<TIterator<NoSwap>> {
        using TSelf = TIterator<NoSwap>;
        using TBase = TComputationValue<TSelf>;

    public:
        TIterator(TMemoryUsageInfo* memInfo, const TStorage& storage)
            : TBase(memInfo)
            , Storage_(storage)
            , Iterator_(Storage_.begin())
            , AtStart_(true)
        {
        }

    private:
        bool Skip() override {
            if (AtStart_) {
                AtStart_ = false;
            } else {
                if (Iterator_ == Storage_.end()) {
                    return false;
                }
                ++Iterator_;
            }

            return Iterator_ != Storage_.end();
        }

        bool Next(NUdf::TUnboxedValue& key) override {
            if (!Skip()) {
                return false;
            }
            if constexpr (IsSet) {
                key = NoSwap ? *Iterator_ : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod::Void());
            } else {
                key = NoSwap ? Iterator_->first : Iterator_->second;
            }
            return true;
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            if (!Next(key)) {
                return false;
            }
            if constexpr (IsSet) {
                payload = NoSwap ? NUdf::TUnboxedValue(NUdf::TUnboxedValuePod::Void()) : *Iterator_;
            } else {
                payload = NoSwap ? Iterator_->second : Iterator_->first;
            }

            return true;
        }

        const TStorage& Storage_;
        typename TStorage::const_iterator Iterator_;
        bool AtStart_;
    };

    class TValue: public TComputationValue<TValue> {
        using TBase = TComputationValue<TValue>;

    public:
        TValue(TMemoryUsageInfo* memInfo, TStorage&& storage)
            : TBase(memInfo)
            , Storage_(std::move(storage))
        {
        }

        bool Contains(const NUdf::TUnboxedValuePod& key) const override {
            return Storage_.contains(key);
        }

        NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
            auto it = Storage_.find(key);
            if (it == Storage_.end()) {
                return {};
            }

            if constexpr (IsSet) {
                return NUdf::TUnboxedValuePod::Void();
            } else {
                return it->second.MakeOptional();
            }
        }

        NUdf::TUnboxedValue GetKeysIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator<true>(this->GetMemInfo(), Storage_));
        }

        NUdf::TUnboxedValue GetDictIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator<true>(this->GetMemInfo(), Storage_));
        }

        NUdf::TUnboxedValue GetPayloadsIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator<false>(this->GetMemInfo(), Storage_));
        }

        ui64 GetDictLength() const override {
            return Storage_.size();
        }

        bool HasDictItems() const override {
            return !Storage_.empty();
        }

    private:
        const TStorage Storage_;
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        return ctx.HolderFactory.Create<TValue>(std::move(storage));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

template <bool IsSet>
class TMutDictInsertWrapper: public TMutableComputationNode<TMutDictInsertWrapper<IsSet>> {
    using TSelf = TMutDictInsertWrapper<IsSet>;
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
        auto key = KeySource_->GetValue(ctx);
        if constexpr (IsSet) {
            auto& set = *static_cast<TMutDictSet*>(input.GetResource());
            set.emplace(key);
        } else {
            auto& map = *static_cast<TMutDictMap*>(input.GetResource());
            auto [it, inserted] = map.emplace(key, NUdf::TUnboxedValue::Invalid());
            if (inserted) {
                auto payload = PayloadSource_->GetValue(ctx);
                it->second = payload;
            }
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

template <bool IsSet>
class TMutDictUpsertWrapper: public TMutableComputationNode<TMutDictUpsertWrapper<IsSet>> {
    using TSelf = TMutDictUpsertWrapper<IsSet>;
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
        auto key = KeySource_->GetValue(ctx);
        if constexpr (IsSet) {
            auto& set = *static_cast<TMutDictSet*>(input.GetResource());
            set.emplace(key);
        } else {
            auto& map = *static_cast<TMutDictMap*>(input.GetResource());
            auto payload = PayloadSource_->GetValue(ctx);
            map[key] = payload;
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

template <bool IsSet>
class TMutDictUpdateWrapper: public TMutableComputationNode<TMutDictUpdateWrapper<IsSet>> {
    using TSelf = TMutDictUpdateWrapper<IsSet>;
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
        if constexpr (!IsSet) {
            auto& map = *static_cast<TMutDictMap*>(input.GetResource());
            auto key = KeySource_->GetValue(ctx);
            auto it = map.find(key);
            if (it != map.cend()) {
                auto payload = PayloadSource_->GetValue(ctx);
                it->second = payload;
            }
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

template <bool IsSet>
class TMutDictRemoveWrapper: public TMutableComputationNode<TMutDictRemoveWrapper<IsSet>> {
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        storage.erase(key);
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

template <bool IsSet>
class TMutDictPopWrapper: public TMutableComputationNode<TMutDictPopWrapper<IsSet>> {
    using TSelf = TMutDictPopWrapper<IsSet>;
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
        auto key = KeySource_->GetValue(ctx);
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        if constexpr (IsSet) {
            auto& set = *static_cast<TMutDictSet*>(input.GetResource());
            auto it = set.find(key);
            if (it != set.cend()) {
                items[1] = TUnboxedValuePod::Void();
                set.erase(it);
            }
        } else {
            auto& map = *static_cast<TMutDictMap*>(input.GetResource());
            auto it = map.find(key);
            if (it != map.cend()) {
                items[1] = it->second.MakeOptional();
                map.erase(it);
            }
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

template <bool IsSet>
class TMutDictContainsWrapper: public TMutableComputationNode<TMutDictContainsWrapper<IsSet>> {
    using TSelf = TMutDictContainsWrapper<IsSet>;
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        auto key = KeySource_->GetValue(ctx);
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        items[1] = NUdf::TUnboxedValuePod(storage.contains(key));
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

template <bool IsSet>
class TMutDictLookupWrapper: public TMutableComputationNode<TMutDictLookupWrapper<IsSet>> {
    using TSelf = TMutDictLookupWrapper<IsSet>;
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
        auto key = KeySource_->GetValue(ctx);
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        if constexpr (IsSet) {
            auto& set = *static_cast<TMutDictSet*>(input.GetResource());
            auto it = set.find(key);
            if (it != set.cend()) {
                items[1] = TUnboxedValuePod::Void();
            }
        } else {
            auto& map = *static_cast<TMutDictMap*>(input.GetResource());
            auto it = map.find(key);
            if (it != map.cend()) {
                items[1] = it->second.MakeOptional();
            }
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

template <bool IsSet>
class TMutDictHasItemsWrapper: public TMutableComputationNode<TMutDictHasItemsWrapper<IsSet>> {
    using TSelf = TMutDictHasItemsWrapper<IsSet>;
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        items[1] = NUdf::TUnboxedValuePod(!storage.empty());
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

template <bool IsSet>
class TMutDictLengthWrapper: public TMutableComputationNode<TMutDictLengthWrapper<IsSet>> {
    using TSelf = TMutDictLengthWrapper<IsSet>;
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        items[0] = input;
        items[1] = NUdf::TUnboxedValuePod(ui64(storage.size()));
        return res;
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Source_);
    }

    TMutDictSupport Support_;
    IComputationNode* const Source_;
};

template <bool IsSet>
class TMutDictItemsWrapper: public TMutableComputationNode<TMutDictItemsWrapper<IsSet>> {
    using TSelf = TMutDictItemsWrapper<IsSet>;
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue* listItems;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        auto list = ctx.HolderFactory.CreateDirectArrayHolder(storage.size(), listItems);
        ui64 index = 0;
        for (auto it = storage.cbegin(); it != storage.cend(); ++it, ++index) {
            NUdf::TUnboxedValue* pairItems;
            auto pair = ctx.HolderFactory.CreateDirectArrayHolder(2, pairItems);
            if constexpr (IsSet) {
                pairItems[0] = *it;
                pairItems[1] = TUnboxedValuePod::Void();
            } else {
                pairItems[0] = it->first;
                pairItems[1] = it->second;
            }

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

template <bool IsSet>
class TMutDictKeysWrapper: public TMutableComputationNode<TMutDictKeysWrapper<IsSet>> {
    using TSelf = TMutDictKeysWrapper<IsSet>;
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue* listItems;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        auto list = ctx.HolderFactory.CreateDirectArrayHolder(storage.size(), listItems);
        ui64 index = 0;
        for (auto it = storage.cbegin(); it != storage.cend(); ++it, ++index) {
            if constexpr (IsSet) {
                listItems[index] = *it;
            } else {
                listItems[index] = it->first;
            }
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

template <bool IsSet>
class TMutDictPayloadsWrapper: public TMutableComputationNode<TMutDictPayloadsWrapper<IsSet>> {
    using TSelf = TMutDictPayloadsWrapper<IsSet>;
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
        auto& storage = *static_cast<TMutDictStorage<IsSet>*>(input.GetResource());
        NUdf::TUnboxedValue* items;
        NUdf::TUnboxedValue* listItems;
        auto res = ctx.HolderFactory.CreateDirectArrayHolder(2, items);
        auto list = ctx.HolderFactory.CreateDirectArrayHolder(storage.size(), listItems);
        if constexpr (IsSet) {
            std::fill_n(listItems, storage.size(), NUdf::TUnboxedValuePod::Void());
        } else {
            ui64 index = 0;
            for (auto it = storage.cbegin(); it != storage.cend(); ++it, ++index) {
                listItems[index] = it->second;
            }
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
    auto payloadType = AS_TYPE(TDictType, callable.GetInput(0).GetStaticType())->GetPayloadType();
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - 1);
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - 1] = LocateNode(ctx.NodeLocator, callable, i);
    }

    auto source = LocateNode(ctx.NodeLocator, callable, 0);
    auto tag = GetMutDictTag(callable.GetType()->GetReturnType());
    if (payloadType->IsVoid()) {
        return new TToMutDictWrapper<true>(ctx.Mutables, keyType, tag, source, std::move(dependentNodes));
    } else {
        return new TToMutDictWrapper<false>(ctx.Mutables, keyType, tag, source, std::move(dependentNodes));
    }
}

IComputationNode* WrapMutDictCreate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1U, "Expected at least 1 arg");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - 1);
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - 1] = LocateNode(ctx.NodeLocator, callable, i);
    }

    auto tag = GetMutDictTag(callable.GetType()->GetReturnType());
    if (payloadType->IsVoid()) {
        return new TMutDictCreateWrapper<true>(ctx.Mutables, keyType, tag, std::move(dependentNodes));
    } else {
        return new TMutDictCreateWrapper<false>(ctx.Mutables, keyType, tag, std::move(dependentNodes));
    }
}

IComputationNode* WrapMutDictInsert(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4U, "Expected 4 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto payloadSource = LocateNode(ctx.NodeLocator, callable, 3);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictInsertWrapper<true>(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
    } else {
        return new TMutDictInsertWrapper<false>(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
    }
}

IComputationNode* WrapMutDictUpsert(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4U, "Expected 4 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto payloadSource = LocateNode(ctx.NodeLocator, callable, 3);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictUpsertWrapper<true>(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
    } else {
        return new TMutDictUpsertWrapper<false>(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
    }
}

IComputationNode* WrapMutDictUpdate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4U, "Expected 4 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto payloadSource = LocateNode(ctx.NodeLocator, callable, 3);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictUpdateWrapper<true>(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
    } else {
        return new TMutDictUpdateWrapper<false>(ctx.Mutables, keyType, tag, source, keySource, payloadSource);
    }
}

IComputationNode* WrapMutDictRemove(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictRemoveWrapper<true>(ctx.Mutables, keyType, tag, source, keySource);
    } else {
        return new TMutDictRemoveWrapper<false>(ctx.Mutables, keyType, tag, source, keySource);
    }
}

IComputationNode* WrapMutDictPop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictPopWrapper<true>(ctx.Mutables, keyType, tag, source, keySource);
    } else {
        return new TMutDictPopWrapper<false>(ctx.Mutables, keyType, tag, source, keySource);
    }
}

IComputationNode* WrapMutDictContains(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictContainsWrapper<true>(ctx.Mutables, keyType, tag, source, keySource);
    } else {
        return new TMutDictContainsWrapper<false>(ctx.Mutables, keyType, tag, source, keySource);
    }
}

IComputationNode* WrapMutDictLookup(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3U, "Expected 3 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto keySource = LocateNode(ctx.NodeLocator, callable, 2);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictLookupWrapper<true>(ctx.Mutables, keyType, tag, source, keySource);
    } else {
        return new TMutDictLookupWrapper<false>(ctx.Mutables, keyType, tag, source, keySource);
    }
}

IComputationNode* WrapMutDictLength(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictLengthWrapper<true>(ctx.Mutables, keyType, tag, source);
    } else {
        return new TMutDictLengthWrapper<false>(ctx.Mutables, keyType, tag, source);
    }
}

IComputationNode* WrapMutDictHasItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictHasItemsWrapper<true>(ctx.Mutables, keyType, tag, source);
    } else {
        return new TMutDictHasItemsWrapper<false>(ctx.Mutables, keyType, tag, source);
    }
}

IComputationNode* WrapMutDictItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictItemsWrapper<true>(ctx.Mutables, keyType, tag, source);
    } else {
        return new TMutDictItemsWrapper<false>(ctx.Mutables, keyType, tag, source);
    }
}

IComputationNode* WrapMutDictKeys(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictKeysWrapper<true>(ctx.Mutables, keyType, tag, source);
    } else {
        return new TMutDictKeysWrapper<false>(ctx.Mutables, keyType, tag, source);
    }
}

IComputationNode* WrapMutDictPayloads(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected 2 args");
    MKQL_ENSURE(callable.GetInput(0).GetNode()->GetType()->IsType(), "Expected type");
    auto keyType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, static_cast<TType*>(callable.GetInput(0).GetNode()))->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 1);
    auto tag = GetMutDictTag(callable.GetInput(1).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TMutDictPayloadsWrapper<true>(ctx.Mutables, keyType, tag, source);
    } else {
        return new TMutDictPayloadsWrapper<false>(ctx.Mutables, keyType, tag, source);
    }
}

IComputationNode* WrapFromMutDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1U, "Expected 1 arg");
    auto keyType = AS_TYPE(TDictType, callable.GetType()->GetReturnType())->GetKeyType();
    auto payloadType = AS_TYPE(TDictType, callable.GetType()->GetReturnType())->GetPayloadType();
    auto source = LocateNode(ctx.NodeLocator, callable, 0);
    auto tag = GetMutDictTag(callable.GetInput(0).GetStaticType());
    if (payloadType->IsVoid()) {
        return new TFromMutDictWrapper<true>(ctx.Mutables, keyType, tag, source);
    } else {
        return new TFromMutDictWrapper<false>(ctx.Mutables, keyType, tag, source);
    }
}

} // namespace NMiniKQL
} // namespace NKikimr

#include "mkql_sort.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/presort.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>

#include <ydb/library/yql/utils/sort.h>

#include <algorithm>
#include <iterator>

namespace NKikimr {
namespace NMiniKQL {

namespace {

std::vector<NUdf::EDataSlot> PrepareKeyTypesByScheme(const std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>>& keySchemeTypes) {
    MKQL_ENSURE(!keySchemeTypes.empty(), "No key types provided");
    std::vector<NUdf::EDataSlot> keyTypes;
    keyTypes.reserve(keySchemeTypes.size());
    for (const auto& schemeType: keySchemeTypes) {
        keyTypes.emplace_back(std::get<0>(schemeType));
        const auto& info = NUdf::GetDataTypeInfo(keyTypes.back());
        MKQL_ENSURE(info.Features & NUdf::CanCompare, "Cannot compare key type: " << info.Name);
    }
    return keyTypes;
}

class TEncoders : public TComputationValue<TEncoders> {
    typedef TComputationValue<TEncoders> TBase;
public:
    TEncoders(TMemoryUsageInfo* memInfo, const std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>>& keySchemeTypes,
        bool allowEncoding)
        : TBase(memInfo)
    {
        Columns.reserve(keySchemeTypes.size());
        for (const auto& x : keySchemeTypes) {
            Columns.push_back(Nothing());
            auto type = std::get<2>(x);
            if (allowEncoding && type) {
                NeedEncode = true;
                Columns.back().ConstructInPlace(type);
            }
        }
    }

    std::vector<TMaybe<TGenericPresortEncoder>> Columns;
    bool NeedEncode = false;
};

class TGatherIteratorRef {
public:
    TGatherIteratorRef(NUdf::TUnboxedValue& first, NUdf::TUnboxedValue& second)
        : First(first)
        , Second(second)
    {}

    operator TKeyPayloadPair () const {
        return TKeyPayloadPair(First, Second);
    }

    TGatherIteratorRef& operator=(const TKeyPayloadPair& rhs) {
        First = rhs.first;
        Second = rhs.second;
        return *this;
    }

    TGatherIteratorRef& operator=(const TGatherIteratorRef& rhs) {
        First = rhs.First;
        Second = rhs.Second;
        return *this;
    }

    friend void swap(TGatherIteratorRef x, TGatherIteratorRef y) {
        std::swap(x.First, y.First);
        std::swap(x.Second, y.Second);
    }

private:
    NUdf::TUnboxedValue& First;
    NUdf::TUnboxedValue& Second;
};

class TGatherIterator : public std::iterator<std::random_access_iterator_tag, TKeyPayloadPair,
    ptrdiff_t, TKeyPayloadPair*, TGatherIteratorRef>
{
public:
    TGatherIterator()
        : First(nullptr)
        , Second(nullptr)
    {
    }

    TGatherIterator(NUdf::TUnboxedValue* first, NUdf::TUnboxedValue* second)
        : First(first)
        , Second(second)
    {}

    TGatherIterator(const TGatherIterator&) = default;
    TGatherIterator& operator=(const TGatherIterator&) = default;
    TGatherIteratorRef operator*() const& {
        return TGatherIteratorRef(*First, *Second);
    }

    TGatherIterator& operator ++ () {
        First++;
        Second++;
        return *this;
    }

    TGatherIterator& operator -- () {
        First--;
        Second--;
        return *this;
    }

    TGatherIterator operator ++ (int) {
        TGatherIterator tmp(*this);
        First++;
        Second++;
        return tmp;
    }

    TGatherIterator operator -- (int) {
        TGatherIterator tmp(*this);
        First--;
        Second--;
        return tmp;
    }

    TGatherIterator& operator += (ptrdiff_t rhs) {
        First += rhs;
        Second += rhs;
        return *this;
    }

    TGatherIterator& operator -= (ptrdiff_t rhs) {
        First -= rhs;
        Second -= rhs;
        return *this;
    }

    ptrdiff_t operator - (TGatherIterator& rhs) const& {
        return First - rhs.First;
    }

    TGatherIterator operator + (ptrdiff_t n) const& {
        TGatherIterator tmp(*this);
        tmp.First += n;
        tmp.Second += n;
        return tmp;
    }

    TGatherIterator operator - (ptrdiff_t n) const& {
        TGatherIterator tmp(*this);
        tmp.First -= n;
        tmp.Second -= n;
        return tmp;
    }

    bool operator==(const TGatherIterator& rhs) const& {
        return First == rhs.First;
    }

    bool operator!=(const TGatherIterator& rhs) const& {
        return First != rhs.First;
    }

    bool operator<(TGatherIterator& rhs) const& {
        return First < rhs.First;
    }

    bool operator<=(TGatherIterator& rhs) const& {
        return First <= rhs.First;
    }

    bool operator>(TGatherIterator& rhs) const& {
        return First > rhs.First;
    }

    bool operator>=(TGatherIterator& rhs) const& {
        return First >= rhs.First;
    }

private:
    NUdf::TUnboxedValue* First;
    NUdf::TUnboxedValue* Second;
};

using TComparator = std::function<bool(const TKeyPayloadPairVector::value_type&, const TKeyPayloadPairVector::value_type&)>;
using TAlgorithm = void(*)(TKeyPayloadPairVector::iterator, TKeyPayloadPairVector::iterator, TComparator);
using TAlgorithmInplace = void(*)(TGatherIterator, TGatherIterator, TComparator);
using TNthAlgorithm = void(*)(TKeyPayloadPairVector::iterator, TKeyPayloadPairVector::iterator, TKeyPayloadPairVector::iterator, TComparator);

struct TCompareDescr {
    TCompareDescr(TComputationMutables& mutables, std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>>&& keySchemeTypes,
        const TVector<NUdf::ICompare::TPtr>& comparators)
        : KeySchemeTypes(std::move(keySchemeTypes))
        , KeyTypes(PrepareKeyTypesByScheme(KeySchemeTypes))
        , Comparators(comparators)
        , Encoders(mutables)
    {}

    static TKeyPayloadPairVector::value_type::first_type& Set(TKeyPayloadPairVector::value_type& item) { return item.first; }
    static TUnboxedValueVector::value_type& Set(TUnboxedValueVector::value_type& item) { return item; }

    static const TKeyPayloadPairVector::value_type::first_type& Get(const TKeyPayloadPairVector::value_type& item) { return item.first; }
    static const TUnboxedValueVector::value_type& Get(const TUnboxedValueVector::value_type& item) { return item; }

    template<class Container>
    std::function<bool(const typename Container::value_type&, const typename Container::value_type&)>
    MakeComparator(const NUdf::TUnboxedValue& ascending) const {
        if (KeyTypes.size() > 1U) {
            // sort tuples
            if (!Comparators.empty()) {
                return [this, &ascending](const typename Container::value_type& x, const typename Container::value_type& y) {
                    const auto& left = Get(x);
                    const auto& right = Get(y);

                    for (ui32 i = 0; i < KeyTypes.size(); ++i) {
                        const auto& leftElem = left.GetElement(i);
                        const auto& rightElem = right.GetElement(i);
                        const bool asc = ascending.GetElement(i).Get<bool>();

                        if (const auto cmp = Comparators[i]->Compare(leftElem, rightElem)) {
                            return asc ? cmp < 0 : cmp > 0;
                        }
                    }

                    return false;
                };
            }

            return [this, &ascending](const typename Container::value_type& x, const typename Container::value_type& y) {
                const auto& left = Get(x);
                const auto& right = Get(y);

                for (ui32 i = 0; i < KeyTypes.size(); ++i) {
                    const auto& keyType = KeyTypes[i];
                    const auto& leftElem = left.GetElement(i);
                    const auto& rightElem = right.GetElement(i);
                    const bool asc = ascending.GetElement(i).Get<bool>();

                    if (const auto cmp = CompareValues(keyType, asc, std::get<1>(KeySchemeTypes[i]), leftElem, rightElem)) {
                        return cmp < 0;
                    }
                }

                return false;
            };
        } else {
            // sort one column
            const bool isOptional = std::get<1>(KeySchemeTypes.front());
            const bool asc = ascending.Get<bool>();

            if (!Comparators.empty()) {
                return [this, asc](const typename Container::value_type& x, const typename Container::value_type& y) {
                    auto cmp = Comparators.front()->Compare(Get(x), Get(y));
                    return asc ? cmp < 0 : cmp > 0;
                };
            }

            return [this, asc, isOptional](const typename Container::value_type& x, const typename Container::value_type& y) {
                    return CompareValues(KeyTypes.front(), asc, isOptional, Get(x), Get(y)) < 0;
            };
        }
    }

    template<class Container>
    void Prepare(TComputationContext& ctx, Container& items) const {
        if (!KeyTypes.empty()) {
            auto& encoders = Encoders.RefMutableObject(ctx, KeySchemeTypes, Comparators.empty());
            for (auto& x : items) {
                PrepareImpl(ctx, x, encoders);
            }
        }
    }

    void PrepareValue(TComputationContext& ctx, NUdf::TUnboxedValue& item) const {
        if (!KeyTypes.empty()) {
            auto& encoders = Encoders.RefMutableObject(ctx, KeySchemeTypes, Comparators.empty());
            PrepareImpl(ctx, item, encoders);
        }
    }

    template <class T>
    void PrepareImpl(TComputationContext& ctx, T& item, TEncoders& encoders) const {
        if (KeyTypes.size() > 1U) {
            // sort tuples
            if (encoders.NeedEncode) {
                NUdf::TUnboxedValue* arrayItems = nullptr;
                NUdf::TUnboxedValue array = ctx.HolderFactory.CreateDirectArrayHolder(KeyTypes.size(), arrayItems);
                for (ui32 i = 0; i < KeyTypes.size(); ++i) {
                    if (auto& e = encoders.Columns[i]) {
                        arrayItems[i] = MakeString(e->Encode(Get(item).GetElement(i), false));
                    } else {
                        arrayItems[i] = Get(item).GetElement(i);
                    }
                }

                Set(item) = std::move(array);
            }
        } else if (auto& encoder = encoders.Columns.front()) {
            Set(item) = MakeString(encoder->Encode(Get(item), false));
        }
    }

    const std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>> KeySchemeTypes;
    const std::vector<NUdf::EDataSlot> KeyTypes;
    const TVector<NUdf::ICompare::TPtr> Comparators;
    TMutableObjectOverBoxedValue<TEncoders> Encoders;
};

template<class TWrapperImpl, bool MaybeInplace>
class TAlgoBaseWrapper : public TMutableComputationNode<TAlgoBaseWrapper<TWrapperImpl, MaybeInplace>> {
    using TBaseComputation = TMutableComputationNode<TAlgoBaseWrapper<TWrapperImpl, MaybeInplace>>;
protected:
    TAlgoBaseWrapper(
            TComputationMutables& mutables,
            std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>>&& keySchemeTypes,
            const TVector<NUdf::ICompare::TPtr>& comparators,
            IComputationNode* list,
            IComputationExternalNode* item,
            IComputationNode* key,
            IComputationNode* ascending,
            bool stealed)
        : TBaseComputation(mutables)
        , Description(mutables, std::move(keySchemeTypes), comparators)
        , List(list)
        , Item(item)
        , Key(key)
        , Ascending(ascending)
        , Stealed(stealed)
    {}

public:
    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& list = List->GetValue(ctx);
        auto ptr = list.GetElements();
        if (MaybeInplace && ptr) {
            TUnboxedValueVector keys;
            NUdf::TUnboxedValue *inplace = nullptr;
            NUdf::TUnboxedValue res;

            auto size = list.GetListLength();
            if (!size) {
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }

            if (Stealed) {
                res = list;
                inplace = const_cast<NUdf::TUnboxedValue*>(ptr);
            } else {
                res = ctx.HolderFactory.CreateDirectArrayHolder(size, inplace);
            }

            keys.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                if (!Stealed) {
                    inplace[i] = ptr[i];
                }

                Item->SetValue(ctx, NUdf::TUnboxedValuePod(ptr[i]));
                keys.emplace_back(Key->GetValue(ctx));
            }

            Description.Prepare(ctx, keys);
            static_cast<const TWrapperImpl*>(this)->PerformInplace(ctx, size, keys.data(), inplace,
                Description.MakeComparator<TKeyPayloadPairVector>(Ascending->GetValue(ctx)));

            return res.Release();
        } else {
            TKeyPayloadPairVector items;
            if (ptr) {
                auto size = list.GetListLength();
                items.reserve(size);
                for (ui32 i = 0; i < size; ++i) {
                    Item->SetValue(ctx, NUdf::TUnboxedValuePod(ptr[i]));
                    items.emplace_back(Key->GetValue(ctx), Item->GetValue(ctx));
                }
            } else {
                const auto& iter = list.GetListIterator();
                if (list.HasFastListLength()) {
                    items.reserve(list.GetListLength());
                }

                for (NUdf::TUnboxedValue item; iter.Next(item);) {
                    Item->SetValue(ctx, std::move(item));
                    items.emplace_back(Key->GetValue(ctx), Item->GetValue(ctx));
                }
            }

            if (items.empty()) {
                return ctx.HolderFactory.GetEmptyContainerLazy();
            }

            Description.Prepare(ctx, items);
            return static_cast<const TWrapperImpl*>(this)->Perform(ctx, items,
                Description.MakeComparator<TKeyPayloadPairVector>(Ascending->GetValue(ctx)));
        }
    }

protected:
    void RegisterDependencies() const override {
        this->DependsOn(List);
        this->Own(Item);
        this->DependsOn(Key);
        this->DependsOn(Ascending);
    }

private:
    TCompareDescr Description;
    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationNode* const Key;
    IComputationNode* const Ascending;
    const bool Stealed;
};

class TAlgoWrapper : public TAlgoBaseWrapper<TAlgoWrapper, true> {
    using TBaseComputation = TAlgoBaseWrapper<TAlgoWrapper, true>;
public:
    TAlgoWrapper(
            TAlgorithm algorithm,
            TAlgorithmInplace algorithmInplace,
            TComputationMutables& mutables,
            std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>>&& keySchemeTypes,
            const TVector<NUdf::ICompare::TPtr>& comparators,
            IComputationNode* list,
            IComputationExternalNode* item,
            IComputationNode* key,
            IComputationNode* ascending,
            bool stealed)
        : TBaseComputation(mutables, std::move(keySchemeTypes), comparators, list, item, key, ascending, stealed)
        , Algorithm(algorithm)
        , AlgorithmInplace(algorithmInplace)
    {}

    NUdf::TUnboxedValuePod Perform(TComputationContext& ctx, TKeyPayloadPairVector& items, const TComparator& comparator) const {
        Algorithm(items.begin(), items.end(), comparator);

        NUdf::TUnboxedValue *inplace = nullptr;
        const auto result = ctx.HolderFactory.CreateDirectArrayHolder(items.size(), inplace);
        for (auto& item : items) {
            *inplace++ = std::move(item.second);
        }
        return result;
    }

    void PerformInplace(TComputationContext&, ui32 size, NUdf::TUnboxedValue* keys, NUdf::TUnboxedValue* items, const TComparator& comparator) const {
        AlgorithmInplace(TGatherIterator(keys, items), TGatherIterator(keys, items) + size, comparator);
    }

private:
    const TAlgorithm Algorithm;
    const TAlgorithmInplace AlgorithmInplace;
};

class TNthAlgoWrapper : public TAlgoBaseWrapper<TNthAlgoWrapper, false> {
    using TBaseComputation = TAlgoBaseWrapper<TNthAlgoWrapper, false>;
public:
    TNthAlgoWrapper(
            TNthAlgorithm algorithm,
            TComputationMutables& mutables,
            std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>>&& keySchemeTypes,
            const TVector<NUdf::ICompare::TPtr>& comparators,
            IComputationNode* list,
            IComputationNode* nth,
            IComputationExternalNode* item,
            IComputationNode* key,
            IComputationNode* ascending)
        : TBaseComputation(mutables, std::move(keySchemeTypes), comparators, list, item, key, ascending, false)
        , Algorithm(algorithm), Nth(nth)
    {}

    NUdf::TUnboxedValuePod Perform(TComputationContext& ctx, TKeyPayloadPairVector& items, const TComparator& comparator) const {
        const auto n = std::min<ui64>(Nth->GetValue(ctx).Get<ui64>(), items.size());
        if (!n) {
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        Algorithm(items.begin(), items.begin() + n, items.end(), comparator);
        items.resize(n);

        NUdf::TUnboxedValue *inplace = nullptr;
        const auto result = ctx.HolderFactory.CreateDirectArrayHolder(n, inplace);
        for (auto& item : items) {
            *inplace++ = std::move(item.second);
        }
        return result;
    }

    void PerformInplace(TComputationContext& ctx, ui32 size, NUdf::TUnboxedValue* keys, NUdf::TUnboxedValue* items, const TComparator& comparator) const {
        Y_UNUSED(ctx);
        Y_UNUSED(size);
        Y_UNUSED(keys);
        Y_UNUSED(items);
        Y_UNUSED(comparator);
        Y_ABORT("Not supported");
    }

private:
    void RegisterDependencies() const final {
        TBaseComputation::RegisterDependencies();
        this->DependsOn(Nth);
    }

    const TNthAlgorithm Algorithm;
    IComputationNode* const Nth;
};

class TKeepTopWrapper : public TMutableComputationNode<TKeepTopWrapper> {
    using TBaseComputation = TMutableComputationNode<TKeepTopWrapper>;
public:
    TKeepTopWrapper(
            TComputationMutables& mutables,
            std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>>&& keySchemeTypes,
            const TVector<NUdf::ICompare::TPtr>& comparators,
            IComputationNode* count,
            IComputationNode* list,
            IComputationNode* item,
            IComputationExternalNode* arg,
            IComputationNode* key,
            IComputationNode* ascending,
            IComputationExternalNode* hotkey)
        : TBaseComputation(mutables)
        , Description(mutables, std::move(keySchemeTypes), comparators)
        , Count(count)
        , List(list)
        , Item(item)
        , Arg(arg)
        , Key(key)
        , Ascending(ascending)
        , HotKey(hotkey)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto count = Count->GetValue(ctx).Get<ui64>();
        if (!count) {
            return ctx.HolderFactory.GetEmptyContainerLazy();
        }

        auto list = List->GetValue(ctx);
        auto item = Item->GetValue(ctx);

        const auto size = list.GetListLength();

        if (size < count) {
            return ctx.HolderFactory.Append(list.Release(), item.Release());
        }
        auto hotkey = HotKey->GetValue(ctx);
        auto hotkey_prepared = hotkey;

        if (!hotkey_prepared.IsInvalid()) {
            Description.PrepareValue(ctx, hotkey_prepared);
        }

        if (size == count) {
            if (hotkey.IsInvalid()) {
                TUnboxedValueVector keys;
                keys.reserve(size);

                const auto ptr = list.GetElements();
                std::transform(ptr, ptr + size, std::back_inserter(keys), [&](const NUdf::TUnboxedValuePod item) {
                    Arg->SetValue(ctx, item);
                    return Key->GetValue(ctx);
                });

                auto keys_copy = keys;

                Description.Prepare(ctx, keys);

                const auto& ascending = Ascending->GetValue(ctx);
                const auto max = std::max_element(keys.begin(), keys.end(), Description.MakeComparator<TUnboxedValueVector>(ascending));
                hotkey_prepared = *max;
                HotKey->SetValue(ctx, std::move(keys_copy[max - keys.begin()]));
            }
        }

        const auto copy = item;
        Arg->SetValue(ctx, item.Release());
        auto key_prepared = Key->GetValue(ctx);
        Description.PrepareValue(ctx, key_prepared);

        const auto& ascending = Ascending->GetValue(ctx);

        if (Description.MakeComparator<TUnboxedValueVector>(ascending)(key_prepared, hotkey_prepared)) {
            const auto reserve = std::max<ui64>(count << 1ULL, 1ULL << 8ULL);
            if (size < reserve) {
                return ctx.HolderFactory.Append(list.Release(), Arg->GetValue(ctx).Release());
            }

            TKeyPayloadPairVector items(1U, TKeyPayloadPair(Key->GetValue(ctx), Arg->GetValue(ctx)));
            items.reserve(items.size() + size);

            const auto ptr = list.GetElements();
            std::transform(ptr, ptr + size, std::back_inserter(items), [&](const NUdf::TUnboxedValuePod item) {
                Arg->SetValue(ctx, item);
                return TKeyPayloadPair(Key->GetValue(ctx), Arg->GetValue(ctx));
            });

            Description.Prepare(ctx, items);
            NYql::FastNthElement(items.begin(), items.begin() + count - 1U, items.end(), Description.MakeComparator<TKeyPayloadPairVector>(ascending));
            items.resize(count);

            NUdf::TUnboxedValue *inplace = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(count, inplace); /// TODO: Use list holder.
            for (auto& item : items) {
                *inplace++ = std::move(item.second);
            }
            return result;
        }

        return list.Release();
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Count);
        DependsOn(List);
        DependsOn(Item);
        Own(Arg);
        DependsOn(Key);
        DependsOn(Ascending);
        Own(HotKey);
    }

    TCompareDescr Description;
    IComputationNode* const Count;
    IComputationNode* const List;
    IComputationNode* const Item;
    IComputationExternalNode* const Arg;
    IComputationNode* const Key;
    IComputationNode* const Ascending;
    IComputationExternalNode* const HotKey;
};

std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>> GetKeySchemeTypes(TType* keyType, TType* ascType) {
    std::vector<std::tuple<NUdf::EDataSlot, bool, TType*>> keySchemeTypes;
    if (ascType->IsTuple()) {
        MKQL_ENSURE(keyType->IsTuple(), "Key must be tuple");
        const auto keyDetailedType = static_cast<TTupleType*>(keyType);
        const auto keyElementsCount = keyDetailedType->GetElementsCount();
        keySchemeTypes.reserve(keyElementsCount);
        for (ui32 i = 0; i < keyElementsCount; ++i) {
            const auto elementType = keyDetailedType->GetElementType(i);
            bool isOptional;
            const auto unpacked = UnpackOptional(elementType, isOptional);
            if (!unpacked->IsData()) {
                keySchemeTypes.emplace_back(NUdf::EDataSlot::String, false, elementType);
            } else {
                keySchemeTypes.emplace_back(*static_cast<TDataType*>(unpacked)->GetDataSlot(), isOptional, nullptr);
            }
        }
    } else {
        keySchemeTypes.reserve(1);
        bool isOptional;
        const auto unpacked = UnpackOptional(keyType, isOptional);
        if (!unpacked->IsData()) {
            keySchemeTypes.emplace_back(NUdf::EDataSlot::String, false, keyType);
        } else {
            keySchemeTypes.emplace_back(*static_cast<TDataType*>(unpacked)->GetDataSlot(), isOptional, nullptr);
        }
    }
    return keySchemeTypes;
}

TVector<NUdf::ICompare::TPtr> MakeComparators(TType* keyType, bool isTuple) {
    if (keyType->IsPresortSupported()) {
        return {};
    }

    if (!isTuple) {
        return { MakeCompareImpl(keyType) };
    } else {
        MKQL_ENSURE(keyType->IsTuple(), "Key must be tuple");
        const auto keyDetailedType = static_cast<TTupleType*>(keyType);
        const auto keyElementsCount = keyDetailedType->GetElementsCount();
        TVector<NUdf::ICompare::TPtr> ret;
        for (ui32 i = 0; i < keyElementsCount; ++i) {
            ret.emplace_back(MakeCompareImpl(keyDetailedType->GetElementType(i)));
        }

        return ret;
    }
}

IComputationNode* WrapAlgo(TAlgorithm algorithm, TAlgorithmInplace algorithmInplace, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

    const auto keyNode = callable.GetInput(2);
    const auto sortNode = callable.GetInput(3);

    const auto keyType = keyNode.GetStaticType();
    const auto ascType = sortNode.GetStaticType();
    auto listNode = callable.GetInput(0);
    IComputationNode* list = nullptr;
    bool stealed = false;
    if (listNode.GetNode()->GetType()->IsCallable()) {
        auto name = AS_TYPE(TCallableType, listNode.GetNode()->GetType())->GetName();
        if (name == "Steal") {
            list = LocateNode(ctx.NodeLocator, static_cast<TCallable&>(*listNode.GetNode()), 0);
            stealed = true;
        }
    }

    if (!list) {
        list = LocateNode(ctx.NodeLocator, callable, 0);
    }

    const auto key = LocateNode(ctx.NodeLocator, callable, 2);
    const auto ascending = LocateNode(ctx.NodeLocator, callable, 3);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);

    auto comparators = MakeComparators(keyType, ascType->IsTuple());
    return new TAlgoWrapper(algorithm, algorithmInplace, ctx.Mutables, GetKeySchemeTypes(keyType, ascType), comparators, list,
        itemArg, key, ascending, stealed);
}

IComputationNode* WrapNthAlgo(TNthAlgorithm algorithm, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const auto keyNode = callable.GetInput(3);
    const auto sortNode = callable.GetInput(4);

    const auto keyType = keyNode.GetStaticType();
    const auto ascType = sortNode.GetStaticType();

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto nth = LocateNode(ctx.NodeLocator, callable, 1);
    const auto key = LocateNode(ctx.NodeLocator, callable, 3);
    const auto ascending = LocateNode(ctx.NodeLocator, callable, 4);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 2);

    auto comparators = MakeComparators(keyType, ascType->IsTuple());
    return new TNthAlgoWrapper(algorithm, ctx.Mutables, GetKeySchemeTypes(keyType, ascType), comparators, list, nth, itemArg, key, ascending);
}

}

IComputationNode* WrapUnstableSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapAlgo(&std::sort<TKeyPayloadPairVector::iterator, TComparator>,
        &std::sort<TGatherIterator, TComparator>, callable, ctx);
}

IComputationNode* WrapSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapAlgo(&std::stable_sort<TKeyPayloadPairVector::iterator, TComparator>,
        &std::stable_sort<TGatherIterator, TComparator>, callable, ctx);
}

IComputationNode* WrapTop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapNthAlgo(&NYql::FastNthElement<TKeyPayloadPairVector::iterator, TComparator>, callable, ctx);
}

IComputationNode* WrapTopSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapNthAlgo(&NYql::FastPartialSort<TKeyPayloadPairVector::iterator, TComparator>, callable, ctx);
}

IComputationNode* WrapKeepTop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    const auto keyNode = callable.GetInput(4);
    const auto sortNode = callable.GetInput(5);

    const auto keyType = keyNode.GetStaticType();
    const auto ascType = sortNode.GetStaticType();

    const auto count = LocateNode(ctx.NodeLocator, callable, 0);
    const auto list = LocateNode(ctx.NodeLocator, callable, 1);
    const auto item = LocateNode(ctx.NodeLocator, callable, 2);

    const auto key = LocateNode(ctx.NodeLocator, callable, 4);
    const auto ascending = LocateNode(ctx.NodeLocator, callable, 5);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 3);
    const auto hotkey = LocateExternalNode(ctx.NodeLocator, callable, 6);

    auto comparators = MakeComparators(keyType, ascType->IsTuple());
    return new TKeepTopWrapper(ctx.Mutables, GetKeySchemeTypes(keyType, ascType), comparators, count, list, item, itemArg, key, ascending, hotkey);
}

}
}

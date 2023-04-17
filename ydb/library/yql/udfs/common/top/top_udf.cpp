#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_type_ops.h>

#include <library/cpp/containers/top_keeper/top_keeper.h>

#include <util/generic/set.h>

#include <algorithm>
#include <iterator>

using namespace NKikimr;
using namespace NUdf;

namespace {

using TUnboxedValuePair = std::pair<TUnboxedValue, TUnboxedValue>;

template <EDataSlot Slot, bool IsTop>
struct TDataCompare {
    bool operator()(const TUnboxedValue& left, const TUnboxedValue& right) const {
        if (IsTop) {
            return CompareValues<Slot>(left, right) > 0;
        } else {
            return CompareValues<Slot>(left, right) < 0;
        }
    }
};

template <EDataSlot Slot, bool IsTop>
struct TDataPairCompare {
    bool operator()(const TUnboxedValuePair& left, const TUnboxedValuePair& right) const {
        if (IsTop) {
            return CompareValues<Slot>(left.first, right.first) > 0;
        } else {
            return CompareValues<Slot>(left.first, right.first) < 0;
        }
    }
};

template <bool IsTop>
struct TGenericCompare {
    ICompare::TPtr Compare;

    bool operator()(const TUnboxedValue& left, const TUnboxedValue& right) const {
        if (IsTop) {
            return Compare->Less(right, left);
        } else {
            return Compare->Less(left, right);
        }
    }
};

template <bool IsTop>
struct TGenericPairCompare {
    ICompare::TPtr Compare;

    bool operator()(const TUnboxedValuePair& left, const TUnboxedValuePair& right) const {
        if (IsTop) {
            return Compare->Less(right.first, left.first);
        } else {
            return Compare->Less(left.first, right.first);
        }
    }
};

template <typename TValue, typename TCompare, typename TAllocator>
class TTopKeeperContainer {
    TTopKeeper<TValue, TCompare, true, TAllocator> Keeper;
    using TOrderedSet = TMultiSet<TValue, TCompare, TAllocator>;
    TMaybe<TOrderedSet> OrderedSet;
    size_t MaxSize = 0;
    bool Finalized = false;
    TCompare Compare;
public:
    explicit TTopKeeperContainer(TCompare compare)
        : Keeper(0, compare)
        , Compare(compare)
    {}

    TVector<TValue, TAllocator> GetInternal() {
        if (OrderedSet) {
            TVector<TValue, TAllocator> result;
            std::copy(OrderedSet->begin(), OrderedSet->end(), std::back_inserter(result));
            return result;
        }
        Finalized = true;
        return Keeper.GetInternal();
    }

    void Insert(const TValue& value) {
        if (MaxSize == 0) {
            return;
        }
        if (Finalized && !OrderedSet) {
            const auto& items = Keeper.Extract();
            OrderedSet = TOrderedSet{items.begin(), items.end(), Compare};
        }
        if (OrderedSet) {
            if (OrderedSet->size() < MaxSize) {
                OrderedSet->insert(value);
                return;
            }
            Y_ENSURE(OrderedSet->size() == MaxSize);
            Y_ENSURE(!OrderedSet->empty());
            auto last = --OrderedSet->end();
            if (Compare(value, *last)) {
                OrderedSet->erase(last);
                OrderedSet->insert(value);
            }
            return;
        }
        Keeper.Insert(value);
    }

    bool IsEmpty() const {
        return OrderedSet ? OrderedSet->empty() : Keeper.IsEmpty();
    }

    size_t GetSize() const {
        return OrderedSet ? OrderedSet->size() : Keeper.GetSize();
    }

    size_t GetMaxSize() const {
        return MaxSize;
    }

    void SetMaxSize(size_t newMaxSize) {
        MaxSize = newMaxSize;
        if (Finalized && !OrderedSet) {
            auto items = Keeper.Extract();
            auto begin = items.begin();
            auto end = begin + Min(MaxSize, items.size());
            OrderedSet = TOrderedSet{begin, end, Compare};
        }
        if (OrderedSet) {
            while (OrderedSet->size() > MaxSize) {
                auto last = --OrderedSet->end();
                OrderedSet->erase(last);
            }
            return;
        }

        Keeper.SetMaxSize(MaxSize);
    }
};

template <typename TCompare>
class TTopKeeperWrapperBase {
protected:
    TTopKeeperContainer<TUnboxedValue, TCompare, TUnboxedValue::TAllocator> Keeper;

protected:
    explicit TTopKeeperWrapperBase(TCompare compare)
        : Keeper(compare)
    {}

    void Init(const TUnboxedValuePod& value, ui32 maxSize) {
        Keeper.SetMaxSize(maxSize);
        AddValue(value);
    }

    void Merge(TTopKeeperWrapperBase& left, TTopKeeperWrapperBase& right) {
        Keeper.SetMaxSize(left.Keeper.GetMaxSize());
        for (const auto& item : left.Keeper.GetInternal()) {
            AddValue(item);
        }
        for (const auto& item : right.Keeper.GetInternal()) {
            AddValue(item);
        }
    }

    void Deserialize(const TUnboxedValuePod& serialized) {
        auto maxSize = serialized.GetElement(0).Get<ui32>();
        auto list = serialized.GetElement(1);

        Keeper.SetMaxSize(maxSize);
        const auto listIter = list.GetListIterator();
        for (TUnboxedValue current; listIter.Next(current);) {
            AddValue(current);
        }
    }

public:
    void AddValue(const TUnboxedValuePod& value) {
        Keeper.Insert(TUnboxedValuePod(value));
    }

    TUnboxedValue Serialize(const IValueBuilder* builder) {
        TUnboxedValue* values = nullptr;
        auto list = builder->NewArray(Keeper.GetSize(), values);

        for (const auto& item : Keeper.GetInternal()) {
            *values++ = item;
        }

        TUnboxedValue* items = nullptr;
        auto result = builder->NewArray(2U, items);
        items[0] = TUnboxedValuePod((ui32)Keeper.GetMaxSize());
        items[1] = list;

        return result;
    }

    TUnboxedValue GetResult(const IValueBuilder* builder) {
        TUnboxedValue* values = nullptr;
        auto list = builder->NewArray(Keeper.GetSize(), values);

        for (const auto& item : Keeper.GetInternal()) {
            *values++ = item;
        }
        return list;
    }
};

template <typename TCompare>
class TTopKeeperPairWrapperBase {
protected:
    TTopKeeperContainer<TUnboxedValuePair, TCompare, TStdAllocatorForUdf<TUnboxedValuePair>> Keeper;

protected:
    explicit TTopKeeperPairWrapperBase(TCompare compare)
        : Keeper(compare)
    {}

    void Init(const TUnboxedValuePod& key, const TUnboxedValuePod& payload, ui32 maxSize) {
        Keeper.SetMaxSize(maxSize);
        AddValue(key, payload);
    }

    void Merge(TTopKeeperPairWrapperBase& left, TTopKeeperPairWrapperBase& right) {
        Keeper.SetMaxSize(left.Keeper.GetMaxSize());
        for (const auto& item : left.Keeper.GetInternal()) {
            AddValue(item.first, item.second);
        }
        for (const auto& item : right.Keeper.GetInternal()) {
            AddValue(item.first, item.second);
        }
    }

    void Deserialize(const TUnboxedValuePod& serialized) {
        auto maxSize = serialized.GetElement(0).Get<ui32>();
        auto list = serialized.GetElement(1);

        Keeper.SetMaxSize(maxSize);
        const auto listIter = list.GetListIterator();
        for (TUnboxedValue current; listIter.Next(current);) {
            AddValue(current.GetElement(0), current.GetElement(1));
        }
    }

public:
    void AddValue(const TUnboxedValuePod& key, const TUnboxedValuePod& payload) {
        Keeper.Insert(std::make_pair(TUnboxedValuePod(key), TUnboxedValuePod(payload)));
    }

    TUnboxedValue Serialize(const IValueBuilder* builder) {
        TUnboxedValue* values = nullptr;
        auto list = builder->NewArray(Keeper.GetSize(), values);

        for (const auto& item : Keeper.GetInternal()) {
            TUnboxedValue* items = nullptr;
            auto pair = builder->NewArray(2U, items);
            items[0] = item.first;
            items[1] = item.second;
            *values++ = pair;
        }

        TUnboxedValue* items = nullptr;
        auto result = builder->NewArray(2U, items);
        items[0] = TUnboxedValuePod((ui32)Keeper.GetMaxSize());
        items[1] = list;

        return result;
    }

    TUnboxedValue GetResult(const IValueBuilder* builder) {
        TUnboxedValue* values = nullptr;
        auto list = builder->NewArray(Keeper.GetSize(), values);

        for (const auto& item : Keeper.GetInternal()) {
            *values++ = item.second;
        }
        return list;
    }
};


template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopKeeperDataWrapper;

template <EDataSlot Slot, bool IsTop>
class TTopKeeperDataWrapper<Slot, false, IsTop>
    : public TTopKeeperWrapperBase<TDataCompare<Slot, IsTop>>
{
public:
    using TBase = TTopKeeperWrapperBase<TDataCompare<Slot, IsTop>>;

    TTopKeeperDataWrapper(const TUnboxedValuePod& value, ui32 maxSize)
        : TBase(TDataCompare<Slot, IsTop>())
    {
        TBase::Init(value, maxSize);
    }

    TTopKeeperDataWrapper(TTopKeeperDataWrapper& left, TTopKeeperDataWrapper& right)
        : TBase(TDataCompare<Slot, IsTop>())
    {
        TBase::Merge(left, right);
    }

    explicit TTopKeeperDataWrapper(const TUnboxedValuePod& serialized)
        : TBase(TDataCompare<Slot, IsTop>())
    {
        TBase::Deserialize(serialized);
    }
};

template <EDataSlot Slot, bool IsTop>
class TTopKeeperDataWrapper<Slot, true, IsTop>
    : public TTopKeeperPairWrapperBase<TDataPairCompare<Slot, IsTop>>
{
public:
    using TBase = TTopKeeperPairWrapperBase<TDataPairCompare<Slot, IsTop>>;

    TTopKeeperDataWrapper(const TUnboxedValuePod& key, const TUnboxedValuePod& payload, ui32 maxSize)
        : TBase(TDataPairCompare<Slot, IsTop>())
    {
        TBase::Init(key, payload, maxSize);
    }

    TTopKeeperDataWrapper(TTopKeeperDataWrapper& left, TTopKeeperDataWrapper& right)
        : TBase(TDataPairCompare<Slot, IsTop>())
    {
        TBase::Merge(left, right);
    }

    explicit TTopKeeperDataWrapper(const TUnboxedValuePod& serialized)
        : TBase(TDataPairCompare<Slot, IsTop>())
    {
        TBase::Deserialize(serialized);
    }
};

template <bool HasKey, bool IsTop>
class TTopKeeperWrapper;

template <bool IsTop>
class TTopKeeperWrapper<false, IsTop>
    : public TTopKeeperWrapperBase<TGenericCompare<IsTop>>
{
public:
    using TBase = TTopKeeperWrapperBase<TGenericCompare<IsTop>>;

    TTopKeeperWrapper(const TUnboxedValuePod& value, ui32 maxSize, ICompare::TPtr compare)
        : TBase(TGenericCompare<IsTop>{compare})
    {
        TBase::Init(value, maxSize);
    }

    TTopKeeperWrapper(TTopKeeperWrapper& left, TTopKeeperWrapper& right, ICompare::TPtr compare)
        : TBase(TGenericCompare<IsTop>{compare})
    {
        TBase::Merge(left, right);
    }

    TTopKeeperWrapper(const TUnboxedValuePod& serialized, ICompare::TPtr compare)
        : TBase(TGenericCompare<IsTop>{compare})
    {
        TBase::Deserialize(serialized);
    }
};

template <bool IsTop>
class TTopKeeperWrapper<true, IsTop>
    : public TTopKeeperPairWrapperBase<TGenericPairCompare<IsTop>>
{
public:
    using TBase = TTopKeeperPairWrapperBase<TGenericPairCompare<IsTop>>;

    TTopKeeperWrapper(const TUnboxedValuePod& key, const TUnboxedValuePod& payload, ui32 maxSize, ICompare::TPtr compare)
        : TBase(TGenericPairCompare<IsTop>{compare})
    {
        TBase::Init(key, payload, maxSize);
    }

    TTopKeeperWrapper(TTopKeeperWrapper& left, TTopKeeperWrapper& right, ICompare::TPtr compare)
        : TBase(TGenericPairCompare<IsTop>{compare})
    {
        TBase::Merge(left, right);
    }

    TTopKeeperWrapper(const TUnboxedValuePod& serialized, ICompare::TPtr compare)
        : TBase(TGenericPairCompare<IsTop>{compare})
    {
        TBase::Deserialize(serialized);
    }
};


template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopResourceData;

template <EDataSlot Slot, bool HasKey, bool IsTop>
TTopResourceData<Slot, HasKey, IsTop>* GetTopResourceData(const TUnboxedValuePod& arg) {
    TTopResourceData<Slot, HasKey, IsTop>::Validate(arg);
    return static_cast<TTopResourceData<Slot, HasKey, IsTop>*>(arg.AsBoxed().Get());
}

template <bool HasKey, bool IsTop>
class TTopResource;

template <bool HasKey, bool IsTop>
TTopResource<HasKey, IsTop>* GetTopResource(const TUnboxedValuePod& arg) {
    TTopResource<HasKey, IsTop>::Validate(arg);
    return static_cast<TTopResource<HasKey, IsTop>*>(arg.AsBoxed().Get());
}


template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopCreateData : public TBoxedValue {
private:
    template <bool HasKey_ = HasKey, typename std::enable_if_t<!HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        return TUnboxedValuePod(
            new TTopResourceData<Slot, HasKey, IsTop>(args[0], args[1].Get<ui32>()));
    }

    template <bool HasKey_ = HasKey, typename std::enable_if_t<HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        return TUnboxedValuePod(
            new TTopResourceData<Slot, HasKey, IsTop>(args[0], args[1], args[2].Get<ui32>()));
    }

    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return RunImpl(args);
    }
};

template <bool HasKey, bool IsTop>
class TTopCreate : public TBoxedValue {
private:
    template <bool HasKey_ = HasKey, typename std::enable_if_t<!HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        return TUnboxedValuePod(
            new TTopResource<HasKey, IsTop>(args[0], args[1].Get<ui32>(), Compare_));
    }

    template <bool HasKey_ = HasKey, typename std::enable_if_t<HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        return TUnboxedValuePod(
            new TTopResource<HasKey, IsTop>(args[0], args[1], args[2].Get<ui32>(), Compare_));
    }

    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return RunImpl(args);
    }

public:
    explicit TTopCreate(ICompare::TPtr compare)
        : Compare_(compare)
    {}

private:
    ICompare::TPtr Compare_;
};

template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopAddValueData : public TBoxedValue {
private:
    template <bool HasKey_ = HasKey, typename std::enable_if_t<!HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        auto resource = GetTopResourceData<Slot, HasKey, IsTop>(args[0]);
        resource->Get()->AddValue(args[1]);
        return TUnboxedValuePod(resource);
    }

    template <bool HasKey_ = HasKey, typename std::enable_if_t<HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        auto resource = GetTopResourceData<Slot, HasKey, IsTop>(args[0]);
        resource->Get()->AddValue(args[1], args[2]);
        return TUnboxedValuePod(resource);
    }

    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return RunImpl(args);
    }
};

template <bool HasKey, bool IsTop>
class TTopAddValue : public TBoxedValue {
private:
    template <bool HasKey_ = HasKey, typename std::enable_if_t<!HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        auto resource = GetTopResource<HasKey, IsTop>(args[0]);
        resource->Get()->AddValue(args[1]);
        return TUnboxedValuePod(resource);
    }

    template <bool HasKey_ = HasKey, typename std::enable_if_t<HasKey_>* = nullptr>
    TUnboxedValue RunImpl(const TUnboxedValuePod* args) const {
        auto resource = GetTopResource<HasKey, IsTop>(args[0]);
        resource->Get()->AddValue(args[1], args[2]);
        return TUnboxedValuePod(resource);
    }

    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return RunImpl(args);
    }

public:
    explicit TTopAddValue(ICompare::TPtr)
    {}
};

template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopSerializeData : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        auto resource = GetTopResourceData<Slot, HasKey, IsTop>(args[0]);
        return resource->Get()->Serialize(valueBuilder);
    }
};

template <bool HasKey, bool IsTop>
class TTopSerialize : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        auto resource = GetTopResource<HasKey, IsTop>(args[0]);
        return resource->Get()->Serialize(valueBuilder);
    }

public:
    explicit TTopSerialize(ICompare::TPtr)
    {}
};

template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopDeserializeData : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return TUnboxedValuePod(new TTopResourceData<Slot, HasKey, IsTop>(args[0]));
    }
};

template <bool HasKey, bool IsTop>
class TTopDeserialize : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return TUnboxedValuePod(new TTopResource<HasKey, IsTop>(args[0], Compare_));
    }

public:
    explicit TTopDeserialize(ICompare::TPtr compare)
        : Compare_(compare)
    {}

private:
    ICompare::TPtr Compare_;
};

template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopMergeData : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto left = GetTopResourceData<Slot, HasKey, IsTop>(args[0]);
        auto right = GetTopResourceData<Slot, HasKey, IsTop>(args[1]);
        return TUnboxedValuePod(new TTopResourceData<Slot, HasKey, IsTop>(*left->Get(), *right->Get()));
    }
};

template <bool HasKey, bool IsTop>
class TTopMerge : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto left = GetTopResource<HasKey, IsTop>(args[0]);
        auto right = GetTopResource<HasKey, IsTop>(args[1]);
        return TUnboxedValuePod(new TTopResource<HasKey, IsTop>(*left->Get(), *right->Get(), Compare_));
    }

public:
    explicit TTopMerge(ICompare::TPtr compare)
        : Compare_(compare)
    {}

private:
    ICompare::TPtr Compare_;
};

template <EDataSlot Slot, bool HasKey, bool IsTop>
class TTopGetResultData : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        auto resource = GetTopResourceData<Slot, HasKey, IsTop>(args[0]);
        return resource->Get()->GetResult(valueBuilder);
    }
};

template <bool HasKey, bool IsTop>
class TTopGetResult : public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        auto resource = GetTopResource<HasKey, IsTop>(args[0]);
        return resource->Get()->GetResult(valueBuilder);
    }

public:
    explicit TTopGetResult(ICompare::TPtr)
    {}
};


#define RESOURCE(slot, hasKey, isTop)                             \
extern const char TopResourceName_##slot##_##hasKey##_##isTop[] = \
    "Top.TopResource."#slot"."#hasKey"."#isTop;                   \
template <>                                                       \
class TTopResourceData<EDataSlot::slot, hasKey, isTop>:           \
    public TBoxedResource<                                        \
        TTopKeeperDataWrapper<EDataSlot::slot, hasKey, isTop>,    \
        TopResourceName_##slot##_##hasKey##_##isTop>              \
{                                                                 \
public:                                                           \
    template <typename... Args>                                   \
    inline TTopResourceData(Args&&... args)                       \
        : TBoxedResource(std::forward<Args>(args)...)             \
    {}                                                            \
};

#define RESOURCE_00(slot, ...) RESOURCE(slot, false, false)
#define RESOURCE_01(slot, ...) RESOURCE(slot, false, true)
#define RESOURCE_10(slot, ...) RESOURCE(slot, true, false)
#define RESOURCE_11(slot, ...) RESOURCE(slot, true, true)

UDF_TYPE_ID_MAP(RESOURCE_00)
UDF_TYPE_ID_MAP(RESOURCE_01)
UDF_TYPE_ID_MAP(RESOURCE_10)
UDF_TYPE_ID_MAP(RESOURCE_11)

#define MAKE_IMPL(operation, slot, hasKey, isTop)                              \
    case EDataSlot::slot:                                                      \
        builder.Implementation(new operation<EDataSlot::slot, hasKey, isTop>); \
        break;

#define CREATE_00(slot, ...) MAKE_IMPL(TTopCreateData, slot, false, false)
#define CREATE_01(slot, ...) MAKE_IMPL(TTopCreateData, slot, false, true)
#define CREATE_10(slot, ...) MAKE_IMPL(TTopCreateData, slot, true, false)
#define CREATE_11(slot, ...) MAKE_IMPL(TTopCreateData, slot, true, true)

#define ADD_VALUE_00(slot, ...) MAKE_IMPL(TTopAddValueData, slot, false, false)
#define ADD_VALUE_01(slot, ...) MAKE_IMPL(TTopAddValueData, slot, false, true)
#define ADD_VALUE_10(slot, ...) MAKE_IMPL(TTopAddValueData, slot, true, false)
#define ADD_VALUE_11(slot, ...) MAKE_IMPL(TTopAddValueData, slot, true, true)

#define MERGE_00(slot, ...) MAKE_IMPL(TTopMergeData, slot, false, false)
#define MERGE_01(slot, ...) MAKE_IMPL(TTopMergeData, slot, false, true)
#define MERGE_10(slot, ...) MAKE_IMPL(TTopMergeData, slot, true, false)
#define MERGE_11(slot, ...) MAKE_IMPL(TTopMergeData, slot, true, true)

#define SERIALIZE_00(slot, ...) MAKE_IMPL(TTopSerializeData, slot, false, false)
#define SERIALIZE_01(slot, ...) MAKE_IMPL(TTopSerializeData, slot, false, true)
#define SERIALIZE_10(slot, ...) MAKE_IMPL(TTopSerializeData, slot, true, false)
#define SERIALIZE_11(slot, ...) MAKE_IMPL(TTopSerializeData, slot, true, true)

#define DESERIALIZE_00(slot, ...) MAKE_IMPL(TTopDeserializeData, slot, false, false)
#define DESERIALIZE_01(slot, ...) MAKE_IMPL(TTopDeserializeData, slot, false, true)
#define DESERIALIZE_10(slot, ...) MAKE_IMPL(TTopDeserializeData, slot, true, false)
#define DESERIALIZE_11(slot, ...) MAKE_IMPL(TTopDeserializeData, slot, true, true)

#define GET_RESULT_00(slot, ...) MAKE_IMPL(TTopGetResultData, slot, false, false)
#define GET_RESULT_01(slot, ...) MAKE_IMPL(TTopGetResultData, slot, false, true)
#define GET_RESULT_10(slot, ...) MAKE_IMPL(TTopGetResultData, slot, true, false)
#define GET_RESULT_11(slot, ...) MAKE_IMPL(TTopGetResultData, slot, true, true)

#define MAKE_TYPE(slot, hasKey, isTop)                                           \
    case EDataSlot::slot:                                                        \
        topType = builder.Resource(TopResourceName_##slot##_##hasKey##_##isTop); \
        break;

#define TYPE_00(slot, ...) MAKE_TYPE(slot, false, false)
#define TYPE_01(slot, ...) MAKE_TYPE(slot, false, true)
#define TYPE_10(slot, ...) MAKE_TYPE(slot, true, false)
#define TYPE_11(slot, ...) MAKE_TYPE(slot, true, true)

#define PARAMETRIZE(action)              \
    if (hasKey) {                        \
        if (isTop) {                     \
            switch (*slot) {             \
            UDF_TYPE_ID_MAP(action##_11) \
            }                            \
        } else {                         \
            switch (*slot) {             \
            UDF_TYPE_ID_MAP(action##_10) \
            }                            \
        }                                \
    } else {                             \
        if (isTop) {                     \
            switch (*slot) {             \
            UDF_TYPE_ID_MAP(action##_01) \
            }                            \
        } else {                         \
            switch (*slot) {             \
            UDF_TYPE_ID_MAP(action##_00) \
            }                            \
        }                                \
    }


#define RESOURCE_GENERIC(hasKey, isTop)                           \
extern const char TopResourceName_Generic_##hasKey##_##isTop[] =  \
    "Top.TopResource.Generic."#hasKey"."#isTop;                   \
template <>                                                       \
class TTopResource<hasKey, isTop>:                                \
    public TBoxedResource<                                        \
        TTopKeeperWrapper<hasKey, isTop>,                         \
        TopResourceName_Generic_##hasKey##_##isTop>               \
{                                                                 \
public:                                                           \
    template <typename... Args>                                   \
    inline TTopResource(Args&&... args)                           \
        : TBoxedResource(std::forward<Args>(args)...)             \
    {}                                                            \
};

RESOURCE_GENERIC(false, false)
RESOURCE_GENERIC(false, true)
RESOURCE_GENERIC(true, false)
RESOURCE_GENERIC(true, true)

#define MAKE_IMPL_GENERIC(operation, hasKey, isTop)                 \
    builder.Implementation(new operation<hasKey, isTop>(compare));

#define CREATE_GENERIC(hasKey, isTop) MAKE_IMPL_GENERIC(TTopCreate, hasKey, isTop)
#define ADD_VALUE_GENERIC(hasKey, isTop) MAKE_IMPL_GENERIC(TTopAddValue, hasKey, isTop)
#define MERGE_GENERIC(hasKey, isTop) MAKE_IMPL_GENERIC(TTopMerge, hasKey, isTop)
#define SERIALIZE_GENERIC(hasKey, isTop) MAKE_IMPL_GENERIC(TTopSerialize, hasKey, isTop)
#define DESERIALIZE_GENERIC(hasKey, isTop) MAKE_IMPL_GENERIC(TTopDeserialize, hasKey, isTop)
#define GET_RESULT_GENERIC(hasKey, isTop) MAKE_IMPL_GENERIC(TTopGetResult, hasKey, isTop)

#define TYPE_GENERIC(hasKey, isTop)                                         \
    topType = builder.Resource(TopResourceName_Generic_##hasKey##_##isTop);

#define PARAMETRIZE_GENERIC(action) \
    if (hasKey) {                   \
        if (isTop) {                \
            action(true, true)      \
        } else {                    \
            action(true, false)     \
        }                           \
    } else {                        \
        if (isTop) {                \
            action(false, true)     \
        } else {                    \
            action(false, false)    \
        }                           \
    }


static const auto CreateName = TStringRef::Of("Create");
static const auto AddValueName = TStringRef::Of("AddValue");
static const auto SerializeName = TStringRef::Of("Serialize");
static const auto DeserializeName = TStringRef::Of("Deserialize");
static const auto MergeName = TStringRef::Of("Merge");
static const auto GetResultName = TStringRef::Of("GetResult");

class TTopModule : public IUdfModule {
public:
    TStringRef Name() const {
        return TStringRef::Of("Top");
    }

    void CleanupOnTerminate() const final {
    }

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(CreateName)->SetTypeAwareness();
        sink.Add(AddValueName)->SetTypeAwareness();
        sink.Add(SerializeName)->SetTypeAwareness();
        sink.Add(DeserializeName)->SetTypeAwareness();
        sink.Add(MergeName)->SetTypeAwareness();
        sink.Add(GetResultName)->SetTypeAwareness();
    }

    void BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef& typeConfig,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const final
    {
        Y_UNUSED(typeConfig);

        try {
            bool typesOnly = (flags & TFlags::TypesOnly);
            builder.UserType(userType);

            if (typeConfig.Size() != 2) {
                builder.SetError(TStringBuilder() << "Invalid type config: " << typeConfig.Data());
                return;
            }

            bool hasKey = (typeConfig.Data()[0] == '1');
            bool isTop = (typeConfig.Data()[1] == '1');

            auto typeHelper = builder.TypeInfoHelper();

            auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() != 3) {
                builder.SetError("User type is not a 3-tuple");
                return;
            }

            auto valueType = userTypeInspector.GetElementType(2);
            auto keyType = valueType;
            auto payloadType = valueType;

            if (hasKey) {
                auto keyPayloadTypeInspector = TTupleTypeInspector(*typeHelper, valueType);
                if (!keyPayloadTypeInspector || keyPayloadTypeInspector.GetElementsCount() != 2) {
                    builder.SetError("Key/payload type is not a 2-tuple");
                    return;
                }

                keyType = keyPayloadTypeInspector.GetElementType(0);
                payloadType = keyPayloadTypeInspector.GetElementType(1);
            }

            bool isGeneric = false;
            ICompare::TPtr compare;
            TMaybe<EDataSlot> slot;

            auto keyTypeInspector = TDataTypeInspector(*typeHelper, keyType);
            if (!keyTypeInspector) {
                isGeneric = true;
                compare = builder.MakeCompare(keyType);
                if (!compare) {
                    return;
                }
            } else {
                slot = FindDataSlot(keyTypeInspector.GetTypeId());
                if (!slot) {
                    builder.SetError("Unknown data type");
                    return;
                }
                if (!(GetDataTypeInfo(*slot).Features & NUdf::CanCompare)) {
                    builder.SetError("Data type is not comparable");
                    return;
                }
            }

            auto serializedListType = builder.List()->Item(valueType).Build();
            auto serializedType = builder.Tuple()->Add<ui32>().Add(serializedListType).Build();

            TType* topType = nullptr;
            if (isGeneric) {
                PARAMETRIZE_GENERIC(TYPE_GENERIC)
            } else {
                PARAMETRIZE(TYPE)
            }

            if (name == CreateName) {
                if (hasKey) {
                    builder.Args()->Add(keyType).Add(payloadType).Add<ui32>().Done().Returns(topType);
                } else {
                    builder.Args()->Add(valueType).Add<ui32>().Done().Returns(topType);
                }

                if (!typesOnly) {
                    if (isGeneric) {
                        PARAMETRIZE_GENERIC(CREATE_GENERIC)
                    } else {
                        PARAMETRIZE(CREATE)
                    }
                }
                builder.IsStrict();
            }

            if (name == AddValueName) {
                if (hasKey) {
                    builder.Args()->Add(topType).Add(keyType).Add(payloadType).Done().Returns(topType);
                } else {
                    builder.Args()->Add(topType).Add(valueType).Done().Returns(topType);
                }

                if (!typesOnly) {
                    if (isGeneric) {
                        PARAMETRIZE_GENERIC(ADD_VALUE_GENERIC)
                    } else {
                        PARAMETRIZE(ADD_VALUE)
                    }
                }
                builder.IsStrict();
            }

            if (name == SerializeName) {
                builder.Args()->Add(topType).Done().Returns(serializedType);

                if (!typesOnly) {
                    if (isGeneric) {
                        PARAMETRIZE_GENERIC(SERIALIZE_GENERIC)
                    } else {
                        PARAMETRIZE(SERIALIZE)
                    }
                }
                builder.IsStrict();
            }

            if (name == DeserializeName) {
                builder.Args()->Add(serializedType).Done().Returns(topType);

                if (!typesOnly) {
                    if (isGeneric) {
                        PARAMETRIZE_GENERIC(DESERIALIZE_GENERIC)
                    } else {
                        PARAMETRIZE(DESERIALIZE)
                    }
                }
            }

            if (name == MergeName) {
                builder.Args()->Add(topType).Add(topType).Done().Returns(topType);

                if (!typesOnly) {
                    if (isGeneric) {
                        PARAMETRIZE_GENERIC(MERGE_GENERIC)
                    } else {
                        PARAMETRIZE(MERGE)
                    }
                }
                builder.IsStrict();
            }

            if (name == GetResultName) {
                auto listType = builder.List()->Item(payloadType).Build();

                builder.Args()->Add(topType).Done().Returns(listType);

                if (!typesOnly) {
                    if (isGeneric) {
                        PARAMETRIZE_GENERIC(GET_RESULT_GENERIC)
                    } else {
                        PARAMETRIZE(GET_RESULT)
                    }
                }
                builder.IsStrict();
            }

        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TTopModule)


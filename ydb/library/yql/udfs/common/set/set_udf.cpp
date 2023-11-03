#include <ydb/library/yql/public/udf/udf_type_ops.h>
#include <ydb/library/yql/public/udf/udf_helpers.h>

#include <unordered_set>

using namespace NKikimr;
using namespace NUdf;

namespace {

template <typename THash, typename TEquals>
class TSetBase {
private:
    std::unordered_set<TUnboxedValue, THash, TEquals, TUnboxedValue::TAllocator> Set;
    ui32 MaxSize = 0;
    bool WasChanged = false;

protected:
    TSetBase(THash hash, TEquals equals)
        : Set(1, hash, equals)
    {}

    void Init(const TUnboxedValuePod& value, ui32 maxSize) {
        MaxSize = maxSize ? maxSize : std::numeric_limits<ui32>::max();
        AddValue(value);
    }

    void Merge(const TSetBase& left, const TSetBase& right) {
        MaxSize = std::max(left.MaxSize, right.MaxSize);
        for (const auto& item : left.Set) {
            AddValue(item);
        }
        for (const auto& item : right.Set) {
            AddValue(item);
        }
    }

    void Deserialize(const TUnboxedValuePod& serialized) {
        MaxSize = serialized.GetElement(0).Get<ui32>();
        auto list = serialized.GetElement(1);

        const auto listIter = list.GetListIterator();
        for (TUnboxedValue current; listIter.Next(current);) {
            AddValue(current);
        }
    }

public:
    void ResetChanged() {
        WasChanged = false;
    }

    bool Changed() const {
        return WasChanged;
    }

    TUnboxedValue Serialize(const IValueBuilder* builder) {
        TUnboxedValue* values = nullptr;
        auto list = builder->NewArray(Set.size(), values);

        for (const auto& item : Set) {
            *values++ = item;
        }

        TUnboxedValue* items = nullptr;
        auto result = builder->NewArray(2U, items);
        items[0] = TUnboxedValuePod(MaxSize);
        items[1] = list;

        return result;
    }

    TUnboxedValue GetResult(const IValueBuilder* builder) {
        TUnboxedValue* values = nullptr;
        auto result = builder->NewArray(Set.size(), values);

        for (const auto& item : Set) {
            *values++ = item;
        }
        return result;
    }

    void AddValue(const TUnboxedValuePod& value) {
        if (Set.size() < MaxSize) {
            WasChanged = Set.insert(TUnboxedValuePod(value)).second;
        }
    }
};

template <EDataSlot Slot>
class TSetData
    : public TSetBase<TUnboxedValueHash<Slot>, TUnboxedValueEquals<Slot>>
{
public:
    using TBase = TSetBase<TUnboxedValueHash<Slot>, TUnboxedValueEquals<Slot>>;

    TSetData(const TUnboxedValuePod& value, ui32 maxSize)
        : TBase(TUnboxedValueHash<Slot>(), TUnboxedValueEquals<Slot>())
    {
        TBase::Init(value, maxSize);
    }

    TSetData(const TSetData& left, const TSetData& right)
        : TBase(TUnboxedValueHash<Slot>(), TUnboxedValueEquals<Slot>())
    {
        TBase::Merge(left, right);
    }

    explicit TSetData(const TUnboxedValuePod& serialized)
        : TBase(TUnboxedValueHash<Slot>(), TUnboxedValueEquals<Slot>())
    {
        TBase::Deserialize(serialized);
    }
};

struct TGenericHash {
    IHash::TPtr Hash;

    std::size_t operator()(const TUnboxedValuePod& value) const {
        return Hash->Hash(value);
    }
};

struct TGenericEquals {
    IEquate::TPtr Equate;

    bool operator()(const TUnboxedValuePod& left, const TUnboxedValuePod& right) const {
        return Equate->Equals(left, right);
    }
};

class TSetGeneric
    : public TSetBase<TGenericHash, TGenericEquals>
{
public:
    using TBase = TSetBase<TGenericHash, TGenericEquals>;

    TSetGeneric(const TUnboxedValuePod& value, ui32 maxSize,
        IHash::TPtr hash, IEquate::TPtr equate)
        : TBase(TGenericHash{hash}, TGenericEquals{equate})
    {
        TBase::Init(value, maxSize);
    }

    TSetGeneric(const TSetGeneric& left, const TSetGeneric& right,
        IHash::TPtr hash, IEquate::TPtr equate)
        : TBase(TGenericHash{hash}, TGenericEquals{equate})
    {
        TBase::Merge(left, right);
    }

    TSetGeneric(const TUnboxedValuePod& serialized,
        IHash::TPtr hash, IEquate::TPtr equate)
        : TBase(TGenericHash{hash}, TGenericEquals{equate})
    {
        TBase::Deserialize(serialized);
    }
};

extern const char SetResourceNameGeneric[] = "Set.SetResource.Generic";
class TSetResource:
    public TBoxedResource<TSetGeneric, SetResourceNameGeneric>
{
public:
    template <typename... Args>
    inline TSetResource(Args&&... args)
        : TBoxedResource(std::forward<Args>(args)...)
    {}
};

template <EDataSlot Slot>
class TSetResourceData;

template <EDataSlot Slot>
TSetResourceData<Slot>* GetSetResourceData(const TUnboxedValuePod& arg) {
    TSetResourceData<Slot>::Validate(arg);
    return static_cast<TSetResourceData<Slot>*>(arg.AsBoxed().Get());
}

TSetResource* GetSetResource(const TUnboxedValuePod& arg) {
    TSetResource::Validate(arg);
    return static_cast<TSetResource*>(arg.AsBoxed().Get());
}


template <EDataSlot Slot>
class TSetCreateData: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return TUnboxedValuePod(new TSetResourceData<Slot>(args[0], args[1].Get<ui32>()));
    }
};

class TSetCreate: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return TUnboxedValuePod(new TSetResource(args[0], args[1].Get<ui32>(), Hash_, Equate_));
    }

public:
    TSetCreate(IHash::TPtr hash, IEquate::TPtr equate)
        : Hash_(hash)
        , Equate_(equate)
    {}

private:
    IHash::TPtr Hash_;
    IEquate::TPtr Equate_;
};

template <EDataSlot Slot>
class TSetAddValueData: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto resource = GetSetResourceData<Slot>(args[0]);
        resource->Get()->ResetChanged();
        resource->Get()->AddValue(args[1]);
        return TUnboxedValuePod(resource);
    }
};

class TSetAddValue: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto resource = GetSetResource(args[0]);
        resource->Get()->ResetChanged();
        resource->Get()->AddValue(args[1]);
        return TUnboxedValuePod(resource);
    }
};

template <EDataSlot Slot>
class TSetWasChangedData: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto resource = GetSetResourceData<Slot>(args[0]);
        return TUnboxedValuePod(resource->Get()->Changed());
    }
};

class TSetWasChanged: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto resource = GetSetResource(args[0]);
        return TUnboxedValuePod(resource->Get()->Changed());
    }
};

template <EDataSlot Slot>
class TSetSerializeData: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        return GetSetResourceData<Slot>(args[0])->Get()->Serialize(valueBuilder);
    }
};

class TSetSerialize: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        return GetSetResource(args[0])->Get()->Serialize(valueBuilder);
    }
};

template <EDataSlot Slot>
class TSetDeserializeData: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return TUnboxedValuePod(new TSetResourceData<Slot>(args[0]));
    }
};

class TSetDeserialize: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        return TUnboxedValuePod(new TSetResource(args[0], Hash_, Equate_));
    }

public:
    TSetDeserialize(IHash::TPtr hash, IEquate::TPtr equate)
        : Hash_(hash)
        , Equate_(equate)
    {}

private:
    IHash::TPtr Hash_;
    IEquate::TPtr Equate_;
};

template <EDataSlot Slot>
class TSetMergeData: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto left = GetSetResourceData<Slot>(args[0]);
        auto right = GetSetResourceData<Slot>(args[1]);
        return TUnboxedValuePod(new TSetResourceData<Slot>(*left->Get(), *right->Get()));
    }
};

class TSetMerge: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const override {
        auto left = GetSetResource(args[0]);
        auto right = GetSetResource(args[1]);
        return TUnboxedValuePod(new TSetResource(*left->Get(), *right->Get(), Hash_, Equate_));
    }

public:
    TSetMerge(IHash::TPtr hash, IEquate::TPtr equate)
        : Hash_(hash)
        , Equate_(equate)
    {}

private:
    IHash::TPtr Hash_;
    IEquate::TPtr Equate_;
};

template <EDataSlot Slot>
class TSetGetResultData: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        return GetSetResourceData<Slot>(args[0])->Get()->GetResult(valueBuilder);
    }
};

class TSetGetResult: public TBoxedValue {
private:
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override {
        return GetSetResource(args[0])->Get()->GetResult(valueBuilder);
    }
};


#define MAKE_RESOURCE(slot, ...)                                            \
extern const char SetResourceName##slot[] = "Set.SetResource."#slot;        \
template <>                                                                 \
class TSetResourceData<EDataSlot::slot>:                                    \
    public TBoxedResource<TSetData<EDataSlot::slot>, SetResourceName##slot> \
{                                                                           \
public:                                                                     \
    template <typename... Args>                                             \
    inline TSetResourceData(Args&&... args)                                 \
        : TBoxedResource(std::forward<Args>(args)...)                       \
    {}                                                                      \
};

UDF_TYPE_ID_MAP(MAKE_RESOURCE)

#define MAKE_IMPL(operation, slot)                          \
case EDataSlot::slot:                                       \
    builder.Implementation(new operation<EDataSlot::slot>); \
    break;

#define MAKE_CREATE(slot, ...) MAKE_IMPL(TSetCreateData, slot)
#define MAKE_ADD_VALUE(slot, ...) MAKE_IMPL(TSetAddValueData, slot)
#define MAKE_WAS_CHANGED(slot, ...) MAKE_IMPL(TSetWasChangedData, slot)
#define MAKE_SERIALIZE(slot, ...) MAKE_IMPL(TSetSerializeData, slot)
#define MAKE_DESERIALIZE(slot, ...) MAKE_IMPL(TSetDeserializeData, slot)
#define MAKE_MERGE(slot, ...) MAKE_IMPL(TSetMergeData, slot)
#define MAKE_GET_RESULT(slot, ...) MAKE_IMPL(TSetGetResultData, slot)

#define MAKE_TYPE(slot, ...)                           \
case EDataSlot::slot:                                  \
    setType = builder.Resource(SetResourceName##slot); \
    break;


static const auto CreateName = TStringRef::Of("Create");
static const auto AddValueName = TStringRef::Of("AddValue");
static const auto WasChangedName = TStringRef::Of("WasChanged"); // must be used right after AddValue
static const auto SerializeName = TStringRef::Of("Serialize");
static const auto DeserializeName = TStringRef::Of("Deserialize");
static const auto MergeName = TStringRef::Of("Merge");
static const auto GetResultName = TStringRef::Of("GetResult");

class TSetModule: public IUdfModule {
public:
    TStringRef Name() const {
        return TStringRef::Of("Set");
    }

    void CleanupOnTerminate() const final {
    }

    void GetAllFunctions(IFunctionsSink& sink) const final {
        sink.Add(CreateName)->SetTypeAwareness();
        sink.Add(AddValueName)->SetTypeAwareness();
        sink.Add(WasChangedName)->SetTypeAwareness();
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
            const bool typesOnly = (flags & TFlags::TypesOnly);
            builder.UserType(userType);

            auto typeHelper = builder.TypeInfoHelper();

            auto userTypeInspector = TTupleTypeInspector(*typeHelper, userType);
            if (!userTypeInspector || userTypeInspector.GetElementsCount() != 3) {
                builder.SetError("User type is not a 3-tuple");
                return;
            }

            bool isGeneric = false;
            IHash::TPtr hash;
            IEquate::TPtr equate;
            TMaybe<EDataSlot> slot;

            auto valueType = userTypeInspector.GetElementType(2);
            auto valueTypeInspector = TDataTypeInspector(*typeHelper, valueType);
            if (!valueTypeInspector) {
                isGeneric = true;
                hash = builder.MakeHash(valueType);
                equate = builder.MakeEquate(valueType);
                if (!hash || !equate) {
                    return;
                }
            } else {
                slot = FindDataSlot(valueTypeInspector.GetTypeId());
                if (!slot) {
                    builder.SetError("Unknown data type");
                    return;
                }
                const auto& info = NUdf::GetDataTypeInfo(*slot);
                const auto& features = info.Features;
                if (!(features & NUdf::CanHash) || !(features & NUdf::CanEquate)) {
                    builder.SetError(TStringBuilder() << "Type " << info.Name << " is not hashable or equatable");
                    return;
                }
            }

            auto serializedListType = builder.List()->Item(valueType).Build();
            auto serializedType = builder.Tuple()->Add<ui32>().Add(serializedListType).Build();

            TType* setType = nullptr;
            if (isGeneric) {
                setType = builder.Resource(SetResourceNameGeneric);
            } else {
                switch (*slot) {
                UDF_TYPE_ID_MAP(MAKE_TYPE)
                }
            }

            if (name == CreateName) {
                builder.IsStrict();

                builder.Args()->Add(valueType).Add<ui32>().Done().Returns(setType);

                if (!typesOnly) {
                    if (isGeneric) {
                        builder.Implementation(new TSetCreate(hash, equate));
                    } else {
                        switch (*slot) {
                        UDF_TYPE_ID_MAP(MAKE_CREATE)
                        }
                    }
                }
            }

            if (name == AddValueName) {
                builder.IsStrict();

                builder.Args()->Add(setType).Add(valueType).Done().Returns(setType);

                if (!typesOnly) {
                    if (isGeneric) {
                        builder.Implementation(new TSetAddValue);
                    } else {
                        switch (*slot) {
                        UDF_TYPE_ID_MAP(MAKE_ADD_VALUE)
                        }
                    }
                }
            }

            if (name == WasChangedName) {
                builder.IsStrict();

                builder.Args()->Add(setType).Done().Returns<bool>();

                if (!typesOnly) {
                    if (isGeneric) {
                        builder.Implementation(new TSetWasChanged);
                    } else {
                        switch (*slot) {
                        UDF_TYPE_ID_MAP(MAKE_WAS_CHANGED)
                        }
                    }
                }
            }

            if (name == MergeName) {
                builder.IsStrict();

                builder.Args()->Add(setType).Add(setType).Done().Returns(setType);

                if (!typesOnly) {
                    if (isGeneric) {
                        builder.Implementation(new TSetMerge(hash, equate));
                    } else {
                        switch (*slot) {
                        UDF_TYPE_ID_MAP(MAKE_MERGE)
                        }
                    }
                }
            }

            if (name == SerializeName) {
                builder.IsStrict();

                builder.Args()->Add(setType).Done().Returns(serializedType);

                if (!typesOnly) {
                    if (isGeneric) {
                        builder.Implementation(new TSetSerialize);
                    } else {
                        switch (*slot) {
                        UDF_TYPE_ID_MAP(MAKE_SERIALIZE)
                        }
                    }
                }
            }

            if (name == DeserializeName) {
                builder.Args()->Add(serializedType).Done().Returns(setType);

                if (!typesOnly) {
                    if (isGeneric) {
                        builder.Implementation(new TSetDeserialize(hash, equate));
                    } else {
                        switch (*slot) {
                        UDF_TYPE_ID_MAP(MAKE_DESERIALIZE)
                        }
                    }
                }
            }

            if (name == GetResultName) {
                auto resultType = builder.List()->Item(valueType).Build();

                builder.IsStrict();

                builder.Args()->Add(setType).Done().Returns(resultType);

                if (!typesOnly) {
                    if (isGeneric) {
                        builder.Implementation(new TSetGetResult);
                    } else {
                        switch (*slot) {
                        UDF_TYPE_ID_MAP(MAKE_GET_RESULT)
                        }
                    }
                }
            }

        } catch (const std::exception& e) {
            builder.SetError(CurrentExceptionMessage());
        }
    }
};

} // namespace

REGISTER_MODULES(TSetModule)

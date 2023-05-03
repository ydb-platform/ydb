#pragma once

#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_types.h>
#include "topfreq.h"
#include <algorithm>
#include <array>

using namespace NYql;
using namespace NUdf;

namespace {
    extern const char TopFreqResourceNameGeneric[] = "TopFreq.TopFreqResource.Generic";
    class TTopFreqResource:
        public TBoxedResource<TTopFreqGeneric, TopFreqResourceNameGeneric>
    {
    public:
        template <typename... Args>
        inline TTopFreqResource(Args&&... args)
            : TBoxedResource(std::forward<Args>(args)...)
        {}
    };

    template <EDataSlot Slot>
    class TTopFreqResourceData;

    template <EDataSlot Slot>
    TTopFreqResourceData<Slot>* GetTopFreqResourceData(const TUnboxedValuePod& arg) {
        TTopFreqResourceData<Slot>::Validate(arg);
        return static_cast<TTopFreqResourceData<Slot>*>(arg.AsBoxed().Get());
    }

    TTopFreqResource* GetTopFreqResource(const TUnboxedValuePod& arg) {
        TTopFreqResource::Validate(arg);
        return static_cast<TTopFreqResource*>(arg.AsBoxed().Get());
    }


    template <EDataSlot Slot>
    class TTopFreq_CreateData: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            ui32 minSize = args[1].Get<ui32>();
            return TUnboxedValuePod(new TTopFreqResourceData<Slot>(args[0], minSize, minSize * 2));
        }
    };

    class TTopFreq_Create: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            ui32 minSize = args[1].Get<ui32>();
            return TUnboxedValuePod(new TTopFreqResource(args[0], minSize, minSize * 2, Hash_, Equate_));
        }

    public:
        TTopFreq_Create(IHash::TPtr hash, IEquate::TPtr equate)
            : Hash_(hash)
            , Equate_(equate)
        {}

    private:
        IHash::TPtr Hash_;
        IEquate::TPtr Equate_;
    };

    template <EDataSlot Slot>
    class TTopFreq_AddValueData: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            const auto topFreq = GetTopFreqResourceData<Slot>(args[0]);
            topFreq->Get()->AddValue(args[1]);
            return TUnboxedValuePod(topFreq);
        }
    };

    class TTopFreq_AddValue: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            const auto topFreq = GetTopFreqResource(args[0]);
            topFreq->Get()->AddValue(args[1]);
            return TUnboxedValuePod(topFreq);
        }
    };

    template <EDataSlot Slot>
    class TTopFreq_SerializeData: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
            return GetTopFreqResourceData<Slot>(args[0])->Get()->Serialize(valueBuilder);
        }
    };

    class TTopFreq_Serialize: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
            return GetTopFreqResource(args[0])->Get()->Serialize(valueBuilder);
        }
    };

    template <EDataSlot Slot>
    class TTopFreq_DeserializeData: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            return TUnboxedValuePod(new TTopFreqResourceData<Slot>(args[0]));
        }
    };

    class TTopFreq_Deserialize: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            return TUnboxedValuePod(new TTopFreqResource(args[0], Hash_, Equate_));
        }

    public:
        TTopFreq_Deserialize(IHash::TPtr hash, IEquate::TPtr equate)
            : Hash_(hash)
            , Equate_(equate)
        {}

    private:
        IHash::TPtr Hash_;
        IEquate::TPtr Equate_;
    };

    template <EDataSlot Slot>
    class TTopFreq_MergeData: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            const auto topFreq0 = GetTopFreqResourceData<Slot>(args[0]);
            const auto topFreq1 = GetTopFreqResourceData<Slot>(args[1]);
            return TUnboxedValuePod(new TTopFreqResourceData<Slot>(*topFreq0->Get(), *topFreq1->Get()));
        }
    };

    class TTopFreq_Merge: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder*, const TUnboxedValuePod* args) const {
            const auto topFreq0 = GetTopFreqResource(args[0]);
            const auto topFreq1 = GetTopFreqResource(args[1]);
            return TUnboxedValuePod(new TTopFreqResource(*topFreq0->Get(), *topFreq1->Get(), Hash_, Equate_));
        }

    public:
        TTopFreq_Merge(IHash::TPtr hash, IEquate::TPtr equate)
            : Hash_(hash)
            , Equate_(equate)
        {}

    private:
        IHash::TPtr Hash_;
        IEquate::TPtr Equate_;
    };

    template <EDataSlot Slot>
    class TTopFreq_GetData: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
            return GetTopFreqResourceData<Slot>(args[0])->Get()->Get(valueBuilder, args[1].Get<ui32>());
        }
    };

    class TTopFreq_Get: public TBoxedValue {
    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const {
            return GetTopFreqResource(args[0])->Get()->Get(valueBuilder, args[1].Get<ui32>());
        }
    };


#define MAKE_RESOURCE(slot, ...)                                                        \
    extern const char TopFreqResourceName##slot[] = "TopFreq.TopFreqResource."#slot;    \
    template <>                                                                         \
    class TTopFreqResourceData<EDataSlot::slot>:                                        \
        public TBoxedResource<TTopFreqData<EDataSlot::slot>, TopFreqResourceName##slot> \
    {                                                                                   \
    public:                                                                             \
        template <typename... Args>                                                     \
        inline TTopFreqResourceData(Args&&... args)                                     \
            : TBoxedResource(std::forward<Args>(args)...)                               \
        {}                                                                              \
    };

    UDF_TYPE_ID_MAP(MAKE_RESOURCE)

#define MAKE_IMPL(operation, slot)                              \
    case EDataSlot::slot:                                       \
        builder.Implementation(new operation<EDataSlot::slot>); \
        break;

#define MAKE_CREATE(slot, ...) MAKE_IMPL(TTopFreq_CreateData, slot)
#define MAKE_ADD_VALUE(slot, ...) MAKE_IMPL(TTopFreq_AddValueData, slot)
#define MAKE_SERIALIZE(slot, ...) MAKE_IMPL(TTopFreq_SerializeData, slot)
#define MAKE_DESERIALIZE(slot, ...) MAKE_IMPL(TTopFreq_DeserializeData, slot)
#define MAKE_MERGE(slot, ...) MAKE_IMPL(TTopFreq_MergeData, slot)
#define MAKE_GET(slot, ...) MAKE_IMPL(TTopFreq_GetData, slot)

#define MAKE_TYPE(slot, ...)                                       \
    case EDataSlot::slot:                                          \
        topFreqType = builder.Resource(TopFreqResourceName##slot); \
        break;


    static const auto CreateName = TStringRef::Of("TopFreq_Create");
    static const auto AddValueName = TStringRef::Of("TopFreq_AddValue");
    static const auto SerializeName = TStringRef::Of("TopFreq_Serialize");
    static const auto DeserializeName = TStringRef::Of("TopFreq_Deserialize");
    static const auto MergeName = TStringRef::Of("TopFreq_Merge");
    static const auto GetName = TStringRef::Of("TopFreq_Get");

    class TTopFreqModule: public IUdfModule {
    public:
        TStringRef Name() const {
            return TStringRef::Of("TopFreq");
        }

        void CleanupOnTerminate() const final {
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(CreateName)->SetTypeAwareness();
            sink.Add(AddValueName)->SetTypeAwareness();
            sink.Add(SerializeName)->SetTypeAwareness();
            sink.Add(DeserializeName)->SetTypeAwareness();
            sink.Add(MergeName)->SetTypeAwareness();
            sink.Add(GetName)->SetTypeAwareness();
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
                    const auto& features = NUdf::GetDataTypeInfo(*slot).Features;
                    if (!(features & NUdf::CanHash) || !(features & NUdf::CanEquate)) {
                        builder.SetError("Data type is not hashable or equatable");
                        return;
                    }
                }

                auto serializedItemType = builder.Tuple()->Add<ui64>().Add(valueType).Build();
                auto serializedListType = builder.List()->Item(serializedItemType).Build();
                auto serializedType = builder.Tuple()->Add<ui32>().Add<ui32>().Add(serializedListType).Build();

                TType* topFreqType = nullptr;
                if (isGeneric) {
                    topFreqType = builder.Resource(TopFreqResourceNameGeneric);
                } else {
                    switch (*slot) {
                    UDF_TYPE_ID_MAP(MAKE_TYPE)
                    }
                }

                if (name == CreateName) {
                    builder.Args()->Add(valueType).Add<ui32>().Done().Returns(topFreqType);

                    if (!typesOnly) {
                        if (isGeneric) {
                            builder.Implementation(new TTopFreq_Create(hash, equate));
                        } else {
                            switch (*slot) {
                            UDF_TYPE_ID_MAP(MAKE_CREATE)
                            }
                        }
                    }
                    builder.IsStrict();
                }

                if (name == AddValueName) {
                    builder.Args()->Add(topFreqType).Add(valueType).Done().Returns(topFreqType);

                    if (!typesOnly) {
                        if (isGeneric) {
                            builder.Implementation(new TTopFreq_AddValue);
                        } else {
                            switch (*slot) {
                            UDF_TYPE_ID_MAP(MAKE_ADD_VALUE)
                            }
                        }
                    }
                    builder.IsStrict();
                }

                if (name == MergeName) {
                    builder.Args()->Add(topFreqType).Add(topFreqType).Done().Returns(topFreqType);

                    if (!typesOnly) {
                        if (isGeneric) {
                            builder.Implementation(new TTopFreq_Merge(hash, equate));
                        } else {
                            switch (*slot) {
                            UDF_TYPE_ID_MAP(MAKE_MERGE)
                            }
                        }
                    }
                    builder.IsStrict();
                }

                if (name == SerializeName) {
                    builder.Args()->Add(topFreqType).Done().Returns(serializedType);

                    if (!typesOnly) {
                        if (isGeneric) {
                            builder.Implementation(new TTopFreq_Serialize);
                        } else {
                            switch (*slot) {
                            UDF_TYPE_ID_MAP(MAKE_SERIALIZE)
                            }
                        }
                    }
                    builder.IsStrict();
                }

                if (name == DeserializeName) {
                    builder.Args()->Add(serializedType).Done().Returns(topFreqType);

                    if (!typesOnly) {
                        if (isGeneric) {
                            builder.Implementation(new TTopFreq_Deserialize(hash, equate));
                        } else {
                            switch (*slot) {
                            UDF_TYPE_ID_MAP(MAKE_DESERIALIZE)
                            }
                        }
                    }
                }

                if (name == GetName) {
                    ui32 indexF, indexV;
                    auto itemType = builder.Struct()->AddField<ui64>("Frequency", &indexF).AddField("Value", valueType, &indexV).Build();
                    auto resultType = builder.List()->Item(itemType).Build();

                    builder.Args()->Add(topFreqType).Add<ui32>().Done().Returns(resultType);

                    if (!typesOnly) {
                        if (isGeneric) {
                            builder.Implementation(new TTopFreq_Get);
                        } else {
                            switch (*slot) {
                            UDF_TYPE_ID_MAP(MAKE_GET)
                            }
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

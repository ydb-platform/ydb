#include <ydb/library/yql/public/udf/udf_registrator.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>
#include <ydb/library/yql/public/udf/udf_type_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>

#include <contrib/libs/croaring/include/roaring/memory.h>
#include <contrib/libs/croaring/include/roaring/roaring.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/yassert.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

    using namespace roaring::api;

    inline roaring_bitmap_t* DeserializePortable(TStringRef binaryString) {
        auto bitmap = roaring_bitmap_portable_deserialize_safe(binaryString.Data(), binaryString.Size());
        Y_ENSURE(bitmap);
        return bitmap;
    }

    struct TRoaringWrapper: public TBoxedValue {
        TRoaringWrapper(TStringRef binaryString)
            : Roaring_(DeserializePortable(binaryString))
        {
        }

        ~TRoaringWrapper() {
            roaring_bitmap_free(Roaring_);
        }

        roaring_bitmap_t* Roaring_;
    };

    inline roaring_bitmap_t* GetBitmapFromArg(TUnboxedValuePod arg) {
        return static_cast<TRoaringWrapper*>(arg.AsBoxed().Get())->Roaring_;
    }

    class TRoaringOrWithBinary: public TBoxedValue {
    public:
        TRoaringOrWithBinary() {
        }

        static TStringRef Name() {
            return TStringRef::Of("OrWithBinary");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);

            if (!args[0] || !args[1]) {
                return TUnboxedValuePod();
            }
            try {
                auto binaryString = args[1].AsStringRef();
                auto bitmap = DeserializePortable(binaryString);

                roaring_bitmap_or_inplace(GetBitmapFromArg(args[0]), bitmap);
                roaring_bitmap_free(bitmap);

                return args[0];
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }
    };

    class TRoaringAndWithBinary: public TBoxedValue {
    public:
        TRoaringAndWithBinary() {
        }

        static TStringRef Name() {
            return TStringRef::Of("AndWithBinary");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);

            if (!args[0] || !args[1]) {
                return TUnboxedValuePod();
            }
            try {
                auto binaryString = args[1].AsStringRef();
                auto bitmap = DeserializePortable(binaryString);

                roaring_bitmap_and_inplace(GetBitmapFromArg(args[0]), bitmap);
                roaring_bitmap_free(bitmap);

                return args[0];
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }
    };

    class TRoaringAnd: public TBoxedValue {
    public:
        TRoaringAnd() {
        }

        static TStringRef Name() {
            return TStringRef::Of("And");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);

            if (!args[0] || !args[1]) {
                return TUnboxedValuePod();
            }
            try {
                roaring_bitmap_and_inplace(GetBitmapFromArg(args[0]), GetBitmapFromArg(args[1]));
                return args[0];
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }
    };

    class TRoaringOr: public TBoxedValue {
    public:
        TRoaringOr() {
        }

        static TStringRef Name() {
            return TStringRef::Of("Or");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);

            if (!args[0] || !args[1]) {
                return TUnboxedValuePod();
            }
            try {
                roaring_bitmap_or_inplace(GetBitmapFromArg(args[0]), GetBitmapFromArg(args[1]));
                return args[0];
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }
    };

    class TRoaringUint32List: public TBoxedValue {
    public:
        TRoaringUint32List(ui32 indexLimit, ui32 indexOffset)
            : IndexLimit_(indexLimit)
            , IndexOffset_(indexOffset)
        {
        }

        static TStringRef Name() {
            return TStringRef::Of("Uint32List");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            if (!args[0]) {
                return TUnboxedValuePod();
            }

            try {
                auto bitmap = GetBitmapFromArg(args[0]);
                auto cardinality = roaring_bitmap_get_cardinality(bitmap);
                auto resultList = TVector<TUnboxedValue>();

                auto limit = cardinality;
                auto offset = 0;

                if (args[1]) {
                    limit = args[1].GetElement(IndexLimit_).Get<ui32>();
                    offset = args[1].GetElement(IndexOffset_).Get<ui32>();
                }
                resultList.reserve(limit);

                auto i = roaring_iterator_create(bitmap);
                while (i->has_value && limit > 0) {
                    if (offset > 0) {
                        offset--;
                    } else {
                        resultList.push_back(TUnboxedValuePod(i->current_value));
                        limit--;
                    }
                    roaring_uint32_iterator_advance(i);
                }
                roaring_uint32_iterator_free(i);
                if (resultList.size() == 0) {
                    return valueBuilder->NewEmptyList();
                }
                return valueBuilder->NewList(resultList.data(), resultList.size());
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }

        ui32 IndexLimit_, IndexOffset_;
    };

    class TRoaringDeserialize: public TBoxedValue {
    public:
        TRoaringDeserialize() {
        }

        static TStringRef Name() {
            return TStringRef::Of("Deserialize");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);

            if (!args[0]) {
                return TUnboxedValuePod();
            }

            try {
                return TUnboxedValuePod(new TRoaringWrapper(args[0].AsStringRef()));
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }
    };

    class TRoaringSerialize: public TBoxedValue {
    public:
        TRoaringSerialize() {
        }

        static TStringRef Name() {
            return TStringRef::Of("Serialize");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            if (!args[0]) {
                return TUnboxedValuePod();
            }

            try {
                auto bitmap = GetBitmapFromArg(args[0]);
                roaring_bitmap_run_optimize(bitmap);

                auto sizeInBytes = roaring_bitmap_portable_size_in_bytes(bitmap);
                auto buf = (char*)UdfAllocateWithSize(sizeInBytes);
                roaring_bitmap_portable_serialize(bitmap, buf);
                auto string = valueBuilder->NewString(TStringRef(buf, sizeInBytes));
                UdfFreeWithSize((void*)buf, sizeInBytes);

                return string;
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }
    };

    class TRoaringCardinality: public TBoxedValue {
    public:
        TRoaringCardinality() {
        }

        static TStringRef Name() {
            return TStringRef::Of("Cardinality");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            if (!args[0]) {
                return TUnboxedValuePod();
            }

            Y_UNUSED(valueBuilder);
            try {
                auto bitmap = GetBitmapFromArg(args[0]);
                auto cardinality = (ui32)roaring_bitmap_get_cardinality(bitmap);
                return TUnboxedValuePod(cardinality);
            } catch (const std::exception& e) {
                UdfTerminate(CurrentExceptionMessage().c_str());
            }
        }
    };

    class TRoaringModule: public IUdfModule {
    public:
        TStringRef Name() const {
            return TStringRef::Of("Roaring");
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(TRoaringSerialize::Name());
            sink.Add(TRoaringDeserialize::Name());

            sink.Add(TRoaringCardinality::Name());

            sink.Add(TRoaringUint32List::Name())->SetTypeAwareness();

            sink.Add(TRoaringOrWithBinary::Name());
            sink.Add(TRoaringOr::Name());

            sink.Add(TRoaringAndWithBinary::Name());
            sink.Add(TRoaringAnd::Name());
        }

        void CleanupOnTerminate() const final {
        }

        void BuildFunctionTypeInfo(const TStringRef& name, NUdf::TType* userType,
                                   const TStringRef& typeConfig, ui32 flags,
                                   IFunctionTypeInfoBuilder& builder) const final {
            try {
                Y_UNUSED(typeConfig);
                Y_UNUSED(userType);

                auto typesOnly = (flags & TFlags::TypesOnly);
                auto roaringType = builder.Resource(RoaringResourceName);
                auto optionalRoaringType = builder.Optional()->Item(roaringType).Build();

                auto optionalStringType =
                    builder.Optional()->Item(builder.SimpleType<char*>()).Build();

                if (TRoaringDeserialize::Name() == name) {
                    builder.Returns(optionalRoaringType).Args()->Add(optionalStringType);

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringDeserialize());
                    }
                } else if (TRoaringSerialize::Name() == name) {
                    builder.Returns(builder.SimpleType<char*>())
                        .Args()
                        ->Add(optionalRoaringType);

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringSerialize());
                    }
                } else if (TRoaringCardinality::Name() == name) {
                    builder.Returns(builder.SimpleType<ui32>())
                        .Args()
                        ->Add(optionalRoaringType);

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringCardinality());
                    }
                } else if (TRoaringUint32List::Name() == name) {
                    auto ui32ListType =
                        builder.List()->Item(builder.SimpleType<ui32>()).Build();

                    ui32 indexLimit = 0, indexOffset = 0;
                    auto structType = builder.Struct()
                                          ->AddField<ui32>("listLimit", &indexLimit)
                                          .AddField<ui32>("listOffset", &indexOffset)
                                          .Build();

                    builder.Returns(builder.Optional()->Item(ui32ListType).Build())
                        .Args()
                        ->Add(optionalRoaringType)
                        .Add(builder.Optional()->Item(structType).Build())
                        .Done()
                        .OptionalArgs(1);

                    if (!typesOnly) {
                        builder.Implementation(
                            new TRoaringUint32List(indexLimit, indexOffset));
                    }
                } else if (TRoaringOrWithBinary::Name() == name) {
                    builder.Returns(optionalRoaringType)
                        .Args()
                        ->Add(optionalRoaringType)
                        .Add(optionalStringType);

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringOrWithBinary());
                    }
                } else if (TRoaringOr::Name() == name) {
                    builder.Returns(optionalRoaringType)
                        .Args()
                        ->Add(optionalRoaringType)
                        .Add(optionalRoaringType);

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringOr());
                    }
                } else if (TRoaringAndWithBinary::Name() == name) {
                    builder.Returns(optionalRoaringType)
                        .Args()
                        ->Add(optionalRoaringType)
                        .Add(optionalStringType);

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAndWithBinary());
                    }
                } else if (TRoaringAnd::Name() == name) {
                    builder.Returns(optionalRoaringType)
                        .Args()
                        ->Add(optionalRoaringType)
                        .Add(optionalRoaringType);

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAnd());
                    }
                } else {
                    TStringBuilder sb;
                    sb << "Unknown function: " << name.Data();
                    builder.SetError(sb);
                }
            } catch (const std::exception& e) {
                builder.SetError(CurrentExceptionMessage());
            }
        }

    private:
        inline static const std::string RoaringResourceName = "roaring_bitmap";

        static void* RoaringMallocUdf(size_t size) {
            auto allocationSize = size + 2 * sizeof(void*);
            auto allocatedMemPointer = UdfAllocateWithSize(allocationSize);

            auto roaringMemPointer = ((char*)allocatedMemPointer) + 2 * sizeof(void*);

            ((void**)roaringMemPointer)[-1] = allocatedMemPointer;
            ((void**)roaringMemPointer)[-2] = ((char*)allocatedMemPointer) + allocationSize;
            return roaringMemPointer;
        }

        static void* RoaringReallocUdf(void* oldPointer, size_t newSize) {
            if (oldPointer == nullptr) {
                return RoaringMallocUdf(newSize);
            }

            if (oldPointer != nullptr && newSize == 0) {
                RoaringFreeUdf(oldPointer);
                return nullptr;
            }

            auto reallocatedPointer = RoaringMallocUdf(newSize);
            auto oldAllocatedMemPointer = (char*)((void**)oldPointer)[-1];
            auto oldSizePointer = (char*)((void**)oldPointer)[-2];
            memcpy(reallocatedPointer, oldPointer, oldSizePointer - oldAllocatedMemPointer);
            RoaringFreeUdf(oldPointer);

            return reallocatedPointer;
        }

        static void* RoaringCallocUdf(size_t elements, size_t elementSize) {
            auto newMem = RoaringMallocUdf(elements * elementSize);
            memset(newMem, 0, elements * elementSize);
            return newMem;
        }

        static void RoaringFreeUdf(void* pointer) {
            if (pointer == nullptr) {
                return;
            }
            auto allocatedMemPointer = (char*)((void**)pointer)[-1];
            auto sizePointer = (char*)((void**)pointer)[-2];
            UdfFreeWithSize(allocatedMemPointer, sizePointer - allocatedMemPointer);
        }

        static void* RoaringAlignedMallocUdf(size_t alignment, size_t size) {
            auto allocationSize = size + (alignment - 1) + 2 * sizeof(void*);
            auto allocatedMemPointer = UdfAllocateWithSize(allocationSize);

            auto roaringMemPointer = ((char*)allocatedMemPointer) + 2 * sizeof(void*);
            if ((size_t)roaringMemPointer & (alignment - 1)) {
                roaringMemPointer += alignment - ((size_t)roaringMemPointer & (alignment - 1));
            }

            ((void**)roaringMemPointer)[-1] = allocatedMemPointer;
            ((void**)roaringMemPointer)[-2] = ((char*)allocatedMemPointer) + allocationSize;
            return roaringMemPointer;
        }
    };

} // namespace

REGISTER_MODULES(TRoaringModule)

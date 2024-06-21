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
            : Roaring(DeserializePortable(binaryString))
        {
        }

        ~TRoaringWrapper() {
            roaring_bitmap_free(Roaring);
        }

        roaring_bitmap_t* Roaring;
    };

    inline roaring_bitmap_t* GetBitmapFromArg(TUnboxedValuePod arg) {
        return static_cast<TRoaringWrapper*>(arg.AsBoxed().Get())->Roaring;
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
            auto binaryString = args[1].AsStringRef();
            auto bitmap = DeserializePortable(binaryString);

            roaring_bitmap_or_inplace(GetBitmapFromArg(args[0]), bitmap);
            roaring_bitmap_free(bitmap);

            return args[0];
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
            auto binaryString = args[1].AsStringRef();
            auto bitmap = DeserializePortable(binaryString);

            roaring_bitmap_and_inplace(GetBitmapFromArg(args[0]), bitmap);
            roaring_bitmap_free(bitmap);

            return args[0];
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
            roaring_bitmap_and_inplace(GetBitmapFromArg(args[0]), GetBitmapFromArg(args[1]));
            return args[0];
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
            roaring_bitmap_or_inplace(GetBitmapFromArg(args[0]), GetBitmapFromArg(args[1]));
            return args[0];
        }
    };

    class TRoaringUint32List: public TBoxedValue {
    public:
        static TStringRef Name() {
            return TStringRef::Of("Uint32List");
        }

    private:
        class TIterator: public TManagedBoxedValue {
        public:
            TIterator(roaring_bitmap_t* Roaring) {
                Iter_ = roaring_iterator_create(Roaring);
            }
            ~TIterator() {
                roaring_uint32_iterator_free(Iter_);
            }
            // Any iterator.
            bool Skip() override {
                if (!Iter_->has_value) {
                    return false;
                }
                roaring_uint32_iterator_advance(Iter_);
                return true;
            };

            // List iterator.
            bool Next(TUnboxedValue& value) override {
                if (!Iter_->has_value) {
                    return false;
                }
                value = TUnboxedValuePod(Iter_->current_value);
                roaring_uint32_iterator_advance(Iter_);
                return true;
            };

        private:
            roaring_uint32_iterator_t* Iter_;
        };

        class TList: public TBoxedValue {
        public:
            TList(TUnboxedValue UnboxedValue)
                : UnboxedValue_(UnboxedValue)
            {
                auto wrapper = static_cast<TRoaringWrapper*>(UnboxedValue_.AsBoxed().Get());
                Length_ = roaring_bitmap_get_cardinality(wrapper->Roaring);
            }

            bool HasFastListLength() const override {
                return true;
            };

            ui64 GetListLength() const override {
                return Length_;
            };

            ui64 GetEstimatedListLength() const override {
                return GetListLength();
            };

            TUnboxedValue GetListIterator() const override {
                auto wrapper = static_cast<TRoaringWrapper*>(UnboxedValue_.AsBoxed().Get());
                return TUnboxedValuePod(new TIterator(wrapper->Roaring));
            };

        private:
            TUnboxedValue UnboxedValue_;
            ui64 Length_;
        };

        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            return TUnboxedValuePod(new TList(args[0]));
        }
    };

    class TRoaringDeserialize: public TBoxedValue {
    public:
        TRoaringDeserialize(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name() {
            return TStringRef::Of("Deserialize");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            try {
                return TUnboxedValuePod(new TRoaringWrapper(args[0].AsStringRef()));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }
        TSourcePosition Pos_;
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
            auto bitmap = GetBitmapFromArg(args[0]);
            roaring_bitmap_run_optimize(bitmap);

            auto sizeInBytes = roaring_bitmap_portable_size_in_bytes(bitmap);

            auto string = valueBuilder->NewStringNotFilled(sizeInBytes);
            roaring_bitmap_portable_serialize(bitmap, string.AsStringRef().Data());

            return string;
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
            Y_UNUSED(valueBuilder);
            auto bitmap = GetBitmapFromArg(args[0]);
            auto cardinality = (ui32)roaring_bitmap_get_cardinality(bitmap);
            return TUnboxedValuePod(cardinality);
        }
    };

    class TRoaringModule: public IUdfModule {
    public:
        TRoaringModule() {
            auto memoryHook = roaring_memory_t{
                RoaringMallocUdf, RoaringReallocUdf, RoaringCallocUdf,
                RoaringFreeUdf, RoaringAlignedMallocUdf, RoaringFreeUdf};
            roaring_init_memory_hook(memoryHook);
        }

        TStringRef Name() const {
            return TStringRef::Of("Roaring");
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(TRoaringSerialize::Name());
            sink.Add(TRoaringDeserialize::Name());

            sink.Add(TRoaringCardinality::Name());

            sink.Add(TRoaringUint32List::Name());

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

                if (TRoaringDeserialize::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>().Args()->Add<TAutoMap<char*>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringDeserialize(builder.GetSourcePosition()));
                    }
                } else if (TRoaringSerialize::Name() == name) {
                    builder.Returns(builder.SimpleType<char*>())
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringSerialize());
                    }
                } else if (TRoaringCardinality::Name() == name) {
                    builder.Returns(builder.SimpleType<ui32>())
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringCardinality());
                    }
                } else if (TRoaringUint32List::Name() == name) {
                    builder.Returns<TListType<ui32>>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringUint32List());
                    }
                } else if (TRoaringOrWithBinary::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<char*>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringOrWithBinary());
                    }
                } else if (TRoaringOr::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<TResource<RoaringResourceName>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringOr());
                    }
                } else if (TRoaringAndWithBinary::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<char*>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAndWithBinary());
                    }
                } else if (TRoaringAnd::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<TResource<RoaringResourceName>>>();

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
        inline static const char RoaringResourceName[] = "roaring_bitmap";

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

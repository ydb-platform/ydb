#include <yql/essentials/public/udf/udf_registrator.h>
#include <yql/essentials/public/udf/udf_terminator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <contrib/libs/croaring/include/roaring/memory.h>
#include <contrib/libs/croaring/include/roaring/roaring.h>

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/generic/singleton.h>
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

        TRoaringWrapper(roaring_bitmap_t* bitmap)
            : Roaring(bitmap)
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

    template <typename Derived>
    class TRoaringOperationBase: public TBoxedValue {
    public:
        TRoaringOperationBase() = default;

        static TStringRef Name() {
            return Derived::GetName();
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            auto* left = GetBitmapFromArg(args[0]);
            auto* right = Derived::GetRightBitmap(args[1]);
            const auto modifySourceBitmap = args[2].GetOrDefault<bool>(false);
            if (modifySourceBitmap) {
                Derived::PerformInplaceOperation(left, right);
                if (Derived::UseBinary()) {
                    roaring_bitmap_free(right);
                }
                return args[0];
            } else {
                auto* result = Derived::PerformOperation(left, right);
                if (Derived::UseBinary()) {
                    roaring_bitmap_free(right);
                }
                return TUnboxedValuePod(new TRoaringWrapper(result));
            }
        }
    };

// Operation implementations
#define DEFINE_ROARING_OP(ClassName, OpName, OpFunc, OpFuncInplace, UseBin)                                         \
    class ClassName: public TRoaringOperationBase<ClassName> {                                                      \
    public:                                                                                                         \
        static constexpr bool UseBinary() {                                                                         \
            return UseBin;                                                                                          \
        }                                                                                                           \
        static const TStringRef GetName() {                                                                         \
            return TStringRef::Of(OpName);                                                                          \
        }                                                                                                           \
                                                                                                                    \
        static roaring_bitmap_t* GetRightBitmap(const TUnboxedValuePod& arg) {                                      \
            if constexpr (UseBin) {                                                                                 \
                return DeserializePortable(arg.AsStringRef());                                                      \
            } else {                                                                                                \
                return GetBitmapFromArg(arg);                                                                       \
            }                                                                                                       \
        }                                                                                                           \
                                                                                                                    \
        static roaring_bitmap_t* PerformOperation(const roaring_bitmap_t* left, const roaring_bitmap_t* right) {    \
            return OpFunc(left, right);                                                                             \
        }                                                                                                           \
                                                                                                                    \
        static void PerformInplaceOperation(roaring_bitmap_t* left, const roaring_bitmap_t* right) {                \
            OpFuncInplace(left, right);                                                                             \
        }                                                                                                           \
    };

    DEFINE_ROARING_OP(TRoaringOrWithBinary, "OrWithBinary", roaring_bitmap_or, roaring_bitmap_or_inplace, true)
    DEFINE_ROARING_OP(TRoaringOr, "Or", roaring_bitmap_or, roaring_bitmap_or_inplace, false)
    DEFINE_ROARING_OP(TRoaringAndWithBinary, "AndWithBinary", roaring_bitmap_and, roaring_bitmap_and_inplace, true)
    DEFINE_ROARING_OP(TRoaringAnd, "And", roaring_bitmap_and, roaring_bitmap_and_inplace, false)
    DEFINE_ROARING_OP(TRoaringAndNotWithBinary, "AndNotWithBinary", roaring_bitmap_andnot, roaring_bitmap_andnot_inplace, true)
    DEFINE_ROARING_OP(TRoaringAndNot, "AndNot", roaring_bitmap_andnot, roaring_bitmap_andnot_inplace, false)

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

    class TRoaringFromUint32List: public TBoxedValue {
    public:
        TRoaringFromUint32List(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name() {
            return TStringRef::Of("FromUint32List");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            roaring_bitmap_t* bitmap = roaring_bitmap_create();
            try {
                roaring_bulk_context_t context = {};

                const auto vector = args[0];
                const auto* elements = vector.GetElements();
                if (elements) {
                    for (auto& value : TArrayRef{elements, vector.GetListLength()}) {
                        roaring_bitmap_add_bulk(bitmap, &context, value.Get<ui32>());
                    }
                } else {
                    TUnboxedValue value;
                    const auto it = vector.GetListIterator();
                    while (it.Next(value)) {
                        roaring_bitmap_add_bulk(bitmap, &context, value.Get<ui32>());
                    }
                }

                return TUnboxedValuePod(new TRoaringWrapper(bitmap));
            } catch (const std::exception& e) {
                roaring_bitmap_free(bitmap);
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }
        TSourcePosition Pos_;
    };

    template <typename Derived>
    class TRoaringNaiveBulkAndBase: public TBoxedValue {
    public:
        TRoaringNaiveBulkAndBase(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name() {
            return Derived::GetFunctionName();
        }

        using Base = TRoaringNaiveBulkAndBase<Derived>;

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            try {
                roaring_bitmap_t* smallest = nullptr;
                std::vector<roaring_bitmap_t*> bitmaps;

                const auto processValue = [&](const TUnboxedValuePod& value) {
                    auto bitmap = Derived::GetBitmap(value);
                    bitmaps.push_back(bitmap);
                    if (!smallest || bitmap->high_low_container.size < smallest->high_low_container.size) {
                        smallest = bitmap;
                    }
                };

                const auto vector = args[0];
                if (const auto* elements = vector.GetElements()) {
                    for (const auto& value : TArrayRef{elements, vector.GetListLength()}) {
                        processValue(value);
                    }
                } else {
                    TUnboxedValue value;
                    const auto it = vector.GetListIterator();
                    while (it.Next(value)) {
                        processValue(value);
                    }
                }

                Y_ENSURE(smallest);

                auto* result = roaring_bitmap_copy(smallest);

                for (const auto& bitmap : bitmaps) {
                    if (bitmap != smallest) {
                        roaring_bitmap_and_inplace(result, bitmap);
                        if (Derived::UseBinary()) {
                            roaring_bitmap_free(bitmap);
                        }
                    }
                }

                if (Derived::UseBinary()) {
                    roaring_bitmap_free(smallest);
                }

                return TUnboxedValuePod(new TRoaringWrapper(result));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    class TRoaringNaiveBulkAnd: public TRoaringNaiveBulkAndBase<TRoaringNaiveBulkAnd> {
    public:
        TRoaringNaiveBulkAnd(TSourcePosition pos)
            : Base(pos)
        {
        }

        static const TStringRef GetFunctionName() {
            return TStringRef::Of("NaiveBulkAnd");
        }

        static roaring_bitmap_t* GetBitmap(const TUnboxedValuePod& value) {
            return GetBitmapFromArg(value);
        }

        static constexpr bool UseBinary() {
            return false;
        }
    };

    class TRoaringNaiveBulkAndWithBinary: public TRoaringNaiveBulkAndBase<TRoaringNaiveBulkAndWithBinary> {
    public:
        TRoaringNaiveBulkAndWithBinary(TSourcePosition pos)
            : Base(pos)
        {
        }

        static const TStringRef GetFunctionName() {
            return TStringRef::Of("NaiveBulkAndWithBinary");
        }

        static roaring_bitmap_t* GetBitmap(const TUnboxedValuePod& value) {
            return DeserializePortable(value.AsStringRef());
        }

        static constexpr bool UseBinary() {
            return true;
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

    class TRoaringRunOptimize: public TBoxedValue {
    public:
        TRoaringRunOptimize() {
        }

        static TStringRef Name() {
            return TStringRef::Of("RunOptimize");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            auto bitmap = GetBitmapFromArg(args[0]);
            roaring_bitmap_run_optimize(bitmap);
            return args[0];
        }
    };

    class TRoaringIntersect: public TBoxedValue {
    public:
        TRoaringIntersect(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name() {
            return TStringRef::Of("Intersect");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            try {
                auto* left = GetBitmapFromArg(args[0]);
                auto* right = GetBitmapFromArg(args[1]);

                return TUnboxedValuePod(roaring_bitmap_intersect(left, right));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    class TRoaringIntersectWithBinary: public TBoxedValue {
    public:
        TRoaringIntersectWithBinary(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name() {
            return TStringRef::Of("IntersectWithBinary");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            try {
                auto* left = GetBitmapFromArg(args[0]);
                auto* right = DeserializePortable(args[1].AsStringRef());

                auto intersect = roaring_bitmap_intersect(left, right);
                roaring_bitmap_free(right);
                return TUnboxedValuePod(intersect);

            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    class TRoaringIsEmpty: public TBoxedValue {
    public:
        TRoaringIsEmpty(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name() {
            return TStringRef::Of("IsEmpty");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            try {
                auto* left = GetBitmapFromArg(args[0]);

                return TUnboxedValuePod(roaring_bitmap_is_empty(left));
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    class TRoaringAdd: public TBoxedValue {
    public:
        TRoaringAdd(TSourcePosition pos)
            : Pos_(pos)
        {
        }

        static TStringRef Name() {
            return TStringRef::Of("Add");
        }

    private:
        TUnboxedValue Run(const IValueBuilder* valueBuilder,
                          const TUnboxedValuePod* args) const override {
            Y_UNUSED(valueBuilder);
            try {
                auto* bitmap = GetBitmapFromArg(args[0]);
                roaring_bitmap_add(bitmap, args[1].Get<ui32>());

                return args[0];
            } catch (const std::exception& e) {
                UdfTerminate((TStringBuilder() << Pos_ << " " << e.what()).data());
            }
        }

        TSourcePosition Pos_;
    };

    class TRoaringModule: public IUdfModule {
    public:
        class TMemoryHookInitializer {
        public:
            TMemoryHookInitializer() {
                auto memoryHook = roaring_memory_t{
                    RoaringMallocUdf, RoaringReallocUdf, RoaringCallocUdf,
                    RoaringFreeUdf, RoaringAlignedMallocUdf, RoaringFreeUdf};
                roaring_init_memory_hook(memoryHook);
            }
        };

        TRoaringModule() {
            Singleton<TMemoryHookInitializer>();
        }

        TStringRef Name() const {
            return TStringRef::Of("Roaring");
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(TRoaringSerialize::Name());
            sink.Add(TRoaringDeserialize::Name());
            sink.Add(TRoaringFromUint32List::Name());

            sink.Add(TRoaringCardinality::Name());

            sink.Add(TRoaringUint32List::Name());

            sink.Add(TRoaringOrWithBinary::Name());
            sink.Add(TRoaringOr::Name());

            sink.Add(TRoaringAndWithBinary::Name());
            sink.Add(TRoaringAnd::Name());
            sink.Add(TRoaringNaiveBulkAnd::Name());
            sink.Add(TRoaringNaiveBulkAndWithBinary::Name());

            sink.Add(TRoaringAndNotWithBinary::Name());
            sink.Add(TRoaringAndNot::Name());

            sink.Add(TRoaringRunOptimize::Name());
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
                    builder.Returns<TResource<RoaringResourceName>>()
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TAutoMap<char*>>()
                        .Add<TOptional<ui32>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringDeserialize(builder.GetSourcePosition()));
                    }
                } else if (TRoaringFromUint32List::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TListType<ui32>>()
                        .Add<TOptional<ui32>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringFromUint32List(builder.GetSourcePosition()));
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
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<char*>>()
                        .Add<TOptional<bool>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringOrWithBinary());
                    }
                } else if (TRoaringOr::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TOptional<bool>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringOr());
                    }
                } else if (TRoaringAndWithBinary::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<char*>>()
                        .Add<TOptional<bool>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAndWithBinary());
                    }
                } else if (TRoaringAnd::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TOptional<bool>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAnd());
                    }
                } else if (TRoaringAndNotWithBinary::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<char*>>()
                        .Add<TOptional<bool>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAndNotWithBinary());
                    }
                } else if (TRoaringAndNot::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .OptionalArgs(1)
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TOptional<bool>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAndNot());
                    }
                } else if (TRoaringRunOptimize::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringRunOptimize());
                    }
                } else if (TRoaringNaiveBulkAnd::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TListType<TOptional<TResource<RoaringResourceName>>>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringNaiveBulkAnd(builder.GetSourcePosition()));
                    }
                } else if (TRoaringNaiveBulkAndWithBinary::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TListType<TOptional<char*>>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringNaiveBulkAndWithBinary(builder.GetSourcePosition()));
                    }
                } else if (TRoaringIntersect::Name() == name) {
                    builder.Returns<bool>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<TResource<RoaringResourceName>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringIntersect(builder.GetSourcePosition()));
                    }
                } else if (TRoaringIntersectWithBinary::Name() == name) {
                    builder.Returns<bool>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<TAutoMap<char*>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringIntersectWithBinary(builder.GetSourcePosition()));
                    }
                } else if (TRoaringIsEmpty::Name() == name) {
                    builder.Returns<bool>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringIsEmpty(builder.GetSourcePosition()));
                    }
                } else if (TRoaringAdd::Name() == name) {
                    builder.Returns<TResource<RoaringResourceName>>()
                        .Args()
                        ->Add<TAutoMap<TResource<RoaringResourceName>>>()
                        .Add<ui32>();

                    if (!typesOnly) {
                        builder.Implementation(new TRoaringAdd(builder.GetSourcePosition()));
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

            // Get the old allocation information.
            auto oldAllocatedMemPointer = ((void**)oldPointer)[-1];
            auto oldSizePointer = ((void**)oldPointer)[-2];

            // Calculate the actual old data size (excluding the header).
            size_t oldSize = (char*)oldSizePointer - (char*)oldAllocatedMemPointer - 2 * sizeof(void*);

            // Allocate new memory.
            auto reallocatedPointer = RoaringMallocUdf(newSize);

            // Copy the minimum of old size and new size.
            size_t copySize = oldSize < newSize ? oldSize : newSize;
            memcpy(reallocatedPointer, oldPointer, copySize);

            // Free the old memory.
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

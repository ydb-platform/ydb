#pragma once

#include "udf_allocator.h"
#include "udf_string.h"
#include "udf_terminator.h"
#include "udf_type_size_check.h"
#include "udf_version.h"

#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <util/system/yassert.h> // FAIL, VERIFY_DEBUG
#include <util/generic/utility.h> // Min, Max
#include <util/generic/yexception.h> // Y_ENSURE
#include <util/system/compiler.h> // Y_FORCE_INLINE

#include <algorithm>
#include <type_traits>

class IOutputStream;

namespace NYql {
namespace NUdf {

class TUnboxedValue;
class TUnboxedValuePod;
class TOpaqueListRepresentation;
class IValueBuilder;

enum class EFetchStatus : ui32 {
    Ok,
    Finish,
    Yield
};

///////////////////////////////////////////////////////////////////////////////
// IApplyContext
///////////////////////////////////////////////////////////////////////////////
class IApplyContext {
public:
    virtual ~IApplyContext() = default;
};

UDF_ASSERT_TYPE_SIZE(IApplyContext, 8);

///////////////////////////////////////////////////////////////////////////////
// IBoxedValue
///////////////////////////////////////////////////////////////////////////////
class IBoxedValue;
using IBoxedValuePtr = TRefCountedPtr<IBoxedValue>;

class IBoxedValue1
{
friend struct TBoxedValueAccessor;
public:
    inline bool IsCompatibleTo(ui16 compatibilityVersion) const {
        return AbiCompatibility_ >= compatibilityVersion;
    }

    IBoxedValue1(const IBoxedValue1&) = delete;
    IBoxedValue1& operator=(const IBoxedValue1&) = delete;
    IBoxedValue1() = default;

    virtual ~IBoxedValue1() = default;

private:
    // List accessors
    virtual bool HasFastListLength() const = 0;
    virtual ui64 GetListLength() const = 0;
    virtual ui64 GetEstimatedListLength() const = 0;
    virtual TUnboxedValue GetListIterator() const = 0;
    // customization of list operations, may return null @{
    virtual const TOpaqueListRepresentation* GetListRepresentation() const = 0;
    virtual IBoxedValuePtr ReverseListImpl(const IValueBuilder& builder) const = 0;
    virtual IBoxedValuePtr SkipListImpl(const IValueBuilder& builder, ui64 count) const = 0;
    virtual IBoxedValuePtr TakeListImpl(const IValueBuilder& builder, ui64 count) const = 0;
    virtual IBoxedValuePtr ToIndexDictImpl(const IValueBuilder& builder) const = 0;
    // @}

    // Dict accessors
    virtual ui64 GetDictLength() const = 0;
    virtual TUnboxedValue GetDictIterator() const = 0;
    virtual TUnboxedValue GetKeysIterator() const = 0; // May return empty.
    virtual TUnboxedValue GetPayloadsIterator() const = 0; // May return empty.
    virtual bool Contains(const TUnboxedValuePod& key) const = 0;
    virtual TUnboxedValue Lookup(const TUnboxedValuePod& key) const = 0;

    // Tuple or Struct accessors
    virtual TUnboxedValue GetElement(ui32 index) const = 0;
    virtual const TUnboxedValue* GetElements() const = 0; // May return nullptr.

    // Callable accessors
    virtual TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const = 0;

    // Resource accessor
    virtual TStringRef GetResourceTag() const = 0;
    virtual void* GetResource() = 0;

    virtual bool HasListItems() const = 0;
    virtual bool HasDictItems() const = 0;

    virtual ui32 GetVariantIndex() const = 0;
    virtual TUnboxedValue GetVariantItem() const = 0;

    // Either Done/Yield with empty result or Ready with non-empty result should be returned
    virtual EFetchStatus Fetch(TUnboxedValue& result) = 0;

    // Any iterator.
    virtual bool Skip() = 0;
    // List iterator.
    virtual bool Next(TUnboxedValue& value) = 0;
    // Dict iterator.
    virtual bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) = 0;

    virtual void Apply(IApplyContext& context) const = 0;

public:
    // reference counting
    inline void Ref() noexcept;
    inline void UnRef() noexcept;
    inline void ReleaseRef() noexcept;
    inline void DeleteUnreferenced() noexcept;
    inline i32 RefCount() const noexcept;
    inline void SetUserMark(ui8 mark) noexcept;
    inline ui8 UserMark() const noexcept;
    inline i32 LockRef() noexcept;
    inline void UnlockRef(i32 prev) noexcept;

private:
    i32 Refs_ = 0;
    const ui16 AbiCompatibility_ = MakeAbiCompatibilityVersion(UDF_ABI_VERSION_MAJOR, UDF_ABI_VERSION_MINOR);
    ui8 UserMark_ = 0;
    const ui8 Reserved_ = 0;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
class IBoxedValue2 : public IBoxedValue1 {
friend struct TBoxedValueAccessor;
private:
    // Save/Load state
    virtual ui32 GetTraverseCount() const = 0;
    virtual TUnboxedValue GetTraverseItem(ui32 index) const = 0;
    virtual TUnboxedValue Save() const = 0;
    virtual void Load(const TStringRef& state) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
class IBoxedValue3 : public IBoxedValue2 {
friend struct TBoxedValueAccessor;
private:
    virtual void Push(const TUnboxedValuePod& value) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
class IBoxedValue4 : public IBoxedValue3 {
friend struct TBoxedValueAccessor;
private:
    virtual bool IsSortedDict() const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
class IBoxedValue5 : public IBoxedValue4 {
friend struct TBoxedValueAccessor;
private:
    virtual void Unused1() = 0;
    virtual void Unused2() = 0;
    virtual void Unused3() = 0;
    virtual void Unused4() = 0;
    virtual void Unused5() = 0;
    virtual void Unused6() = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
class IBoxedValue6 : public IBoxedValue5 {
friend struct TBoxedValueAccessor;
private:
    virtual EFetchStatus WideFetch(TUnboxedValue* result, ui32 width) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
class IBoxedValue7 : public IBoxedValue6 {
friend struct TBoxedValueAccessor;
private:
    virtual bool Load2(const TUnboxedValue& state) = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
class IBoxedValue : public IBoxedValue7 {
protected:
    IBoxedValue();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
class IBoxedValue : public IBoxedValue6 {
protected:
    IBoxedValue();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
class IBoxedValue : public IBoxedValue5 {
protected:
    IBoxedValue();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
class IBoxedValue : public IBoxedValue4 {
protected:
    IBoxedValue();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
class IBoxedValue : public IBoxedValue3 {
protected:
    IBoxedValue();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
class IBoxedValue : public IBoxedValue2 {
protected:
    IBoxedValue();
};
#else
class IBoxedValue : public IBoxedValue1 {
protected:
    IBoxedValue();
};
#endif

UDF_ASSERT_TYPE_SIZE(IBoxedValue, 16);

UDF_ASSERT_TYPE_SIZE(IBoxedValuePtr, 8);

///////////////////////////////////////////////////////////////////////////////
// TBoxedValueAccessor
///////////////////////////////////////////////////////////////////////////////
struct TBoxedValueAccessor
{
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)

#define METHOD_MAP(xx) \
    xx(HasFastListLength) \
    xx(GetListLength) \
    xx(GetEstimatedListLength) \
    xx(GetListIterator) \
    xx(GetListRepresentation) \
    xx(ReverseListImpl) \
    xx(SkipListImpl) \
    xx(TakeListImpl) \
    xx(ToIndexDictImpl) \
    xx(GetDictLength) \
    xx(GetDictIterator) \
    xx(GetKeysIterator) \
    xx(GetPayloadsIterator) \
    xx(Contains) \
    xx(Lookup) \
    xx(GetElement) \
    xx(GetElements) \
    xx(Run) \
    xx(GetResourceTag) \
    xx(GetResource) \
    xx(HasListItems) \
    xx(HasDictItems) \
    xx(GetVariantIndex) \
    xx(GetVariantItem) \
    xx(Fetch) \
    xx(Skip) \
    xx(Next) \
    xx(NextPair) \
    xx(Apply) \
    xx(GetTraverseCount) \
    xx(GetTraverseItem) \
    xx(Save) \
    xx(Load) \
    xx(Push) \
    xx(IsSortedDict) \
    xx(Unused1) \
    xx(Unused2) \
    xx(Unused3) \
    xx(Unused4) \
    xx(Unused5) \
    xx(Unused6) \
    xx(WideFetch) \
    xx(Load2)

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)

#define METHOD_MAP(xx) \
    xx(HasFastListLength) \
    xx(GetListLength) \
    xx(GetEstimatedListLength) \
    xx(GetListIterator) \
    xx(GetListRepresentation) \
    xx(ReverseListImpl) \
    xx(SkipListImpl) \
    xx(TakeListImpl) \
    xx(ToIndexDictImpl) \
    xx(GetDictLength) \
    xx(GetDictIterator) \
    xx(GetKeysIterator) \
    xx(GetPayloadsIterator) \
    xx(Contains) \
    xx(Lookup) \
    xx(GetElement) \
    xx(GetElements) \
    xx(Run) \
    xx(GetResourceTag) \
    xx(GetResource) \
    xx(HasListItems) \
    xx(HasDictItems) \
    xx(GetVariantIndex) \
    xx(GetVariantItem) \
    xx(Fetch) \
    xx(Skip) \
    xx(Next) \
    xx(NextPair) \
    xx(Apply) \
    xx(GetTraverseCount) \
    xx(GetTraverseItem) \
    xx(Save) \
    xx(Load) \
    xx(Push) \
    xx(IsSortedDict) \
    xx(Unused1) \
    xx(Unused2) \
    xx(Unused3) \
    xx(Unused4) \
    xx(Unused5) \
    xx(Unused6) \
    xx(WideFetch) \

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)

#define METHOD_MAP(xx) \
    xx(HasFastListLength) \
    xx(GetListLength) \
    xx(GetEstimatedListLength) \
    xx(GetListIterator) \
    xx(GetListRepresentation) \
    xx(ReverseListImpl) \
    xx(SkipListImpl) \
    xx(TakeListImpl) \
    xx(ToIndexDictImpl) \
    xx(GetDictLength) \
    xx(GetDictIterator) \
    xx(GetKeysIterator) \
    xx(GetPayloadsIterator) \
    xx(Contains) \
    xx(Lookup) \
    xx(GetElement) \
    xx(GetElements) \
    xx(Run) \
    xx(GetResourceTag) \
    xx(GetResource) \
    xx(HasListItems) \
    xx(HasDictItems) \
    xx(GetVariantIndex) \
    xx(GetVariantItem) \
    xx(Fetch) \
    xx(Skip) \
    xx(Next) \
    xx(NextPair) \
    xx(Apply) \
    xx(GetTraverseCount) \
    xx(GetTraverseItem) \
    xx(Save) \
    xx(Load) \
    xx(Push) \
    xx(IsSortedDict) \
    xx(Unused1) \
    xx(Unused2) \
    xx(Unused3) \
    xx(Unused4) \
    xx(Unused5) \
    xx(Unused6)

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)

#define METHOD_MAP(xx) \
    xx(HasFastListLength) \
    xx(GetListLength) \
    xx(GetEstimatedListLength) \
    xx(GetListIterator) \
    xx(GetListRepresentation) \
    xx(ReverseListImpl) \
    xx(SkipListImpl) \
    xx(TakeListImpl) \
    xx(ToIndexDictImpl) \
    xx(GetDictLength) \
    xx(GetDictIterator) \
    xx(GetKeysIterator) \
    xx(GetPayloadsIterator) \
    xx(Contains) \
    xx(Lookup) \
    xx(GetElement) \
    xx(GetElements) \
    xx(Run) \
    xx(GetResourceTag) \
    xx(GetResource) \
    xx(HasListItems) \
    xx(HasDictItems) \
    xx(GetVariantIndex) \
    xx(GetVariantItem) \
    xx(Fetch) \
    xx(Skip) \
    xx(Next) \
    xx(NextPair) \
    xx(Apply) \
    xx(GetTraverseCount) \
    xx(GetTraverseItem) \
    xx(Save) \
    xx(Load) \
    xx(Push) \
    xx(IsSortedDict)

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)

#define METHOD_MAP(xx) \
    xx(HasFastListLength) \
    xx(GetListLength) \
    xx(GetEstimatedListLength) \
    xx(GetListIterator) \
    xx(GetListRepresentation) \
    xx(ReverseListImpl) \
    xx(SkipListImpl) \
    xx(TakeListImpl) \
    xx(ToIndexDictImpl) \
    xx(GetDictLength) \
    xx(GetDictIterator) \
    xx(GetKeysIterator) \
    xx(GetPayloadsIterator) \
    xx(Contains) \
    xx(Lookup) \
    xx(GetElement) \
    xx(GetElements) \
    xx(Run) \
    xx(GetResourceTag) \
    xx(GetResource) \
    xx(HasListItems) \
    xx(HasDictItems) \
    xx(GetVariantIndex) \
    xx(GetVariantItem) \
    xx(Fetch) \
    xx(Skip) \
    xx(Next) \
    xx(NextPair) \
    xx(Apply) \
    xx(GetTraverseCount) \
    xx(GetTraverseItem) \
    xx(Save) \
    xx(Load) \
    xx(Push)

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)

#define METHOD_MAP(xx) \
    xx(HasFastListLength) \
    xx(GetListLength) \
    xx(GetEstimatedListLength) \
    xx(GetListIterator) \
    xx(GetListRepresentation) \
    xx(ReverseListImpl) \
    xx(SkipListImpl) \
    xx(TakeListImpl) \
    xx(ToIndexDictImpl) \
    xx(GetDictLength) \
    xx(GetDictIterator) \
    xx(GetKeysIterator) \
    xx(GetPayloadsIterator) \
    xx(Contains) \
    xx(Lookup) \
    xx(GetElement) \
    xx(GetElements) \
    xx(Run) \
    xx(GetResourceTag) \
    xx(GetResource) \
    xx(HasListItems) \
    xx(HasDictItems) \
    xx(GetVariantIndex) \
    xx(GetVariantItem) \
    xx(Fetch) \
    xx(Skip) \
    xx(Next) \
    xx(NextPair) \
    xx(Apply) \
    xx(GetTraverseCount) \
    xx(GetTraverseItem) \
    xx(Save) \
    xx(Load)

#else

#define METHOD_MAP(xx) \
    xx(HasFastListLength) \
    xx(GetListLength) \
    xx(GetEstimatedListLength) \
    xx(GetListIterator) \
    xx(GetListRepresentation) \
    xx(ReverseListImpl) \
    xx(SkipListImpl) \
    xx(TakeListImpl) \
    xx(ToIndexDictImpl) \
    xx(GetDictLength) \
    xx(GetDictIterator) \
    xx(GetKeysIterator) \
    xx(GetPayloadsIterator) \
    xx(Contains) \
    xx(Lookup) \
    xx(GetElement) \
    xx(GetElements) \
    xx(Run) \
    xx(GetResourceTag) \
    xx(GetResource) \
    xx(HasListItems) \
    xx(HasDictItems) \
    xx(GetVariantIndex) \
    xx(GetVariantItem) \
    xx(Fetch) \
    xx(Skip) \
    xx(Next) \
    xx(NextPair) \
    xx(Apply)

#endif

    enum class EMethod : ui32 {
#define MAP_HANDLER(xx) xx,
        METHOD_MAP(MAP_HANDLER)
#undef MAP_HANDLER
    };

    template<typename Method>
    static uintptr_t GetMethodPtr(Method method) {
        uintptr_t ret;
        memcpy(&ret, &method, sizeof(uintptr_t));
        return ret;
    }

    static uintptr_t GetMethodPtr(EMethod method) {
        switch (method) {
#define MAP_HANDLER(xx) case EMethod::xx: return GetMethodPtr(&IBoxedValue::xx);
            METHOD_MAP(MAP_HANDLER)
#undef MAP_HANDLER
        }

        Y_ABORT("unknown method");
    }

    template<EMethod Method> static uintptr_t GetMethodPtr();

    // List accessors
    static inline bool HasFastListLength(const IBoxedValue& value);
    static inline ui64 GetListLength(const IBoxedValue& value);
    static inline ui64 GetEstimatedListLength(const IBoxedValue& value);
    static inline TUnboxedValue GetListIterator(const IBoxedValue& value);
    // customization of list operations, may return null @{
    static inline const TOpaqueListRepresentation* GetListRepresentation(const IBoxedValue& value);
    static inline IBoxedValuePtr ReverseListImpl(const IBoxedValue& value, const IValueBuilder& builder);
    static inline IBoxedValuePtr SkipListImpl(const IBoxedValue& value, const IValueBuilder& builder, ui64 count);
    static inline IBoxedValuePtr TakeListImpl(const IBoxedValue& value, const IValueBuilder& builder, ui64 count);
    static inline IBoxedValuePtr ToIndexDictImpl(const IBoxedValue& value, const IValueBuilder& builder);
    // @}

    // Dict accessors
    static inline ui64 GetDictLength(const IBoxedValue& value);
    static inline TUnboxedValue GetDictIterator(const IBoxedValue& value);
    static inline TUnboxedValue GetKeysIterator(const IBoxedValue& value);
    static inline TUnboxedValue GetPayloadsIterator(const IBoxedValue& value);
    static inline bool Contains(const IBoxedValue& value, const TUnboxedValuePod& key);
    static inline TUnboxedValue Lookup(const IBoxedValue& value, const TUnboxedValuePod& key);

    // Tuple or Struct accessors
    static inline TUnboxedValue GetElement(const IBoxedValue& value, ui32 index);
    static inline const TUnboxedValue* GetElements(const IBoxedValue& value);

    // Callable accessors
    static inline TUnboxedValue Run(const IBoxedValue& value, const IValueBuilder* valueBuilder, const TUnboxedValuePod* args);

    // Resource accessor
    static inline TStringRef GetResourceTag(const IBoxedValue& value);
    static inline void* GetResource(IBoxedValue& value);

    static inline bool HasListItems(const IBoxedValue& value);
    static inline bool HasDictItems(const IBoxedValue& value);

    static inline ui32 GetVariantIndex(const IBoxedValue& value);
    static inline TUnboxedValue GetVariantItem(const IBoxedValue& value);

    static inline EFetchStatus Fetch(IBoxedValue& value, TUnboxedValue& result);

    // Any iterator.
    static inline bool Skip(IBoxedValue& value);
    // List iterator.
    static inline bool Next(IBoxedValue& value, TUnboxedValue& result);
    // Dict iterator.
    static inline bool NextPair(IBoxedValue& value, TUnboxedValue& key, TUnboxedValue& payload);

    static inline void Apply(IBoxedValue& value, IApplyContext& context);

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
    // Save/Load state
    static inline ui32 GetTraverseCount(const IBoxedValue& value);
    static inline TUnboxedValue GetTraverseItem(const IBoxedValue& value, ui32 index);
    static inline TUnboxedValue Save(const IBoxedValue& value);
    static inline void Load(IBoxedValue& value, const TStringRef& state);
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
    static inline void Push(IBoxedValue& value, const TUnboxedValuePod& data);
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
    static inline bool IsSortedDict(IBoxedValue& value);
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
    static inline EFetchStatus WideFetch(IBoxedValue& value, TUnboxedValue* result, ui32 width);
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
    static inline bool Load2(IBoxedValue& value, const TUnboxedValue& data);
#endif
};

#define MAP_HANDLER(xx) template<> inline uintptr_t TBoxedValueAccessor::GetMethodPtr<TBoxedValueAccessor::EMethod::xx>() { return GetMethodPtr(&IBoxedValue::xx); }
METHOD_MAP(MAP_HANDLER)
#undef MAP_HANDLER

#undef METHOD_MAP

///////////////////////////////////////////////////////////////////////////////
// TBoxedValue
///////////////////////////////////////////////////////////////////////////////
class TBoxedValueBase: public IBoxedValue {
private:
    // List accessors
    bool HasFastListLength() const override;
    ui64 GetListLength() const override;
    ui64 GetEstimatedListLength() const override;
    TUnboxedValue GetListIterator() const override;
    const TOpaqueListRepresentation* GetListRepresentation() const override;
    IBoxedValuePtr ReverseListImpl(const IValueBuilder& builder) const override;
    IBoxedValuePtr SkipListImpl(const IValueBuilder& builder, ui64 count) const override;
    IBoxedValuePtr TakeListImpl(const IValueBuilder& builder, ui64 count) const override;
    IBoxedValuePtr ToIndexDictImpl(const IValueBuilder& builder) const override;

    // Dict accessors
    ui64 GetDictLength() const override;
    TUnboxedValue GetDictIterator() const override;
    TUnboxedValue GetKeysIterator() const override;
    TUnboxedValue GetPayloadsIterator() const override;
    bool Contains(const TUnboxedValuePod& key) const override;
    TUnboxedValue Lookup(const TUnboxedValuePod& key) const override;

    // Tuple or Struct accessors
    TUnboxedValue GetElement(ui32 index) const override;
    const TUnboxedValue* GetElements() const override;

    // Callable accessors
    TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const override;

    // Resource accessors
    TStringRef GetResourceTag() const override;
    void* GetResource() override;

    bool HasListItems() const override;
    bool HasDictItems() const override;

    ui32 GetVariantIndex() const override;
    TUnboxedValue GetVariantItem() const override;

    EFetchStatus Fetch(TUnboxedValue& result) override;

    // Any iterator.
    bool Skip() override;
    // List iterator.
    bool Next(TUnboxedValue& value) override;
    // Dict iterator.
    bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) override;

    void Apply(IApplyContext& context) const override;

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
    // Save/Load state
    ui32 GetTraverseCount() const override;
    TUnboxedValue GetTraverseItem(ui32 index) const override;
    TUnboxedValue Save() const override;
    void Load(const TStringRef& state) override;
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
    void Push(const TUnboxedValuePod& value) override;
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
    bool IsSortedDict() const override;
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
    void Unused1() override;
    void Unused2() override;
    void Unused3() override;
    void Unused4() override;
    void Unused5() override;
    void Unused6() override;
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
    EFetchStatus WideFetch(TUnboxedValue* result, ui32 width) override;
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
    bool Load2(const TUnboxedValue& value) override;
#endif
};

class TBoxedValueLink: public TBoxedValueBase
{
public:
    void Link(TBoxedValueLink* root);
    void Unlink();
    void InitLinks() { Left = Right = this; }
    TBoxedValueLink* GetLeft() const { return Left; }
    TBoxedValueLink* GetRight() const { return Right; }
private:
    TBoxedValueLink *Left = nullptr;
    TBoxedValueLink *Right = nullptr;
};

class TBoxedValue: public TBoxedValueLink, public TWithUdfAllocator
{
public:
    TBoxedValue();
    ~TBoxedValue();
};

class TManagedBoxedValue: public TBoxedValueBase, public TWithUdfAllocator
{
};

UDF_ASSERT_TYPE_SIZE(TBoxedValue, 32);

///////////////////////////////////////////////////////////////////////////////
// TUnboxedValuePod
///////////////////////////////////////////////////////////////////////////////

struct TRawEmbeddedValue {
    char Buffer[0xE];
    ui8 Size;
    ui8 Meta;
};

struct TRawBoxedValue {
    IBoxedValue* Value;
    ui8 Reserved[7];
    ui8 Meta;
};

struct TRawStringValue {
    static constexpr ui32 OffsetLimit = 1 << 24;

    TStringValue::TData* Value;
    ui32 Size;
    union {
        struct {
            ui8 Skip[3];
            ui8 Meta;
        };
        ui32 Offset;
    };
};

class TUnboxedValuePod
{
friend class TUnboxedValue;
public:
    enum class EMarkers : ui8 {
        Empty = 0,
        Embedded,
        String,
        Boxed,
    };

    inline TUnboxedValuePod() noexcept = default;
    inline ~TUnboxedValuePod() noexcept = default;

    inline TUnboxedValuePod(const TUnboxedValuePod& value) noexcept = default;
    inline TUnboxedValuePod(TUnboxedValuePod&& value) noexcept = default;

    inline TUnboxedValuePod& operator=(const TUnboxedValuePod& value) noexcept = default;
    inline TUnboxedValuePod& operator=(TUnboxedValuePod&& value) noexcept = default;

    inline TUnboxedValuePod(TUnboxedValue&&) = delete;
    inline TUnboxedValuePod& operator=(TUnboxedValue&&) = delete;

    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    inline explicit TUnboxedValuePod(T value);
    inline explicit TUnboxedValuePod(IBoxedValuePtr&& value);
    inline explicit TUnboxedValuePod(TStringValue&& value, ui32 size = Max<ui32>(), ui32 offset = 0U);

    void Dump(IOutputStream& out) const;
    // meta information
    inline explicit operator bool() const { return bool(Raw); }

    inline bool HasValue() const { return EMarkers::Empty != Raw.GetMarkers(); }

    inline bool IsString() const { return EMarkers::String == Raw.GetMarkers(); }
    inline bool IsBoxed() const { return EMarkers::Boxed == Raw.GetMarkers(); }
    inline bool IsEmbedded() const { return EMarkers::Embedded == Raw.GetMarkers(); }

    // Data accessors
    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result || std::is_same_v<T, NYql::NDecimal::TInt128>>>
    inline T Get() const;
    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    inline T GetOrDefault(T ifEmpty) const;

    inline explicit TUnboxedValuePod(NYql::NDecimal::TInt128 value);
    inline explicit TUnboxedValuePod(NYql::NDecimal::TUint128 value);
    inline NYql::NDecimal::TInt128 GetInt128() const;
    inline NYql::NDecimal::TUint128 GetUint128() const;

    inline const void* GetRawPtr() const;
    inline void* GetRawPtr();

    inline TStringRef AsStringRef() const&;
    inline TMutableStringRef AsStringRef() &;
    void AsStringRef() && = delete;

    inline TStringValue AsStringValue() const;
    inline IBoxedValuePtr AsBoxed() const;
    inline TStringValue::TData* AsRawStringValue() const;
    inline IBoxedValue* AsRawBoxed() const;
    inline bool UniqueBoxed() const;

    // special values
    inline static TUnboxedValuePod Void();
    inline static TUnboxedValuePod Zero();
    inline static TUnboxedValuePod Embedded(ui8 size);
    inline static TUnboxedValuePod Embedded(const TStringRef& value);
    inline static TUnboxedValuePod Invalid();
    inline static TUnboxedValuePod MakeFinish();
    inline static TUnboxedValuePod MakeYield();
    inline bool IsInvalid() const;
    inline bool IsFinish() const;
    inline bool IsYield() const;
    inline bool IsSpecial() const;

    inline TUnboxedValuePod MakeOptional() const;
    inline TUnboxedValuePod GetOptionalValue() const;

    template<bool IsOptional> inline TUnboxedValuePod GetOptionalValueIf() const;
    template<bool IsOptional> inline TUnboxedValuePod MakeOptionalIf() const;

    // List accessors
    inline bool HasFastListLength() const;
    inline ui64 GetListLength() const;
    inline ui64 GetEstimatedListLength() const;
    inline TUnboxedValue GetListIterator() const;
    inline bool HasListItems() const;
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
    inline void Push(const TUnboxedValuePod& value) const;
#endif

    // Dict accessors
    inline ui64 GetDictLength() const;
    inline TUnboxedValue GetDictIterator() const;
    inline TUnboxedValue GetKeysIterator() const;
    inline TUnboxedValue GetPayloadsIterator() const;

    inline bool Contains(const TUnboxedValuePod& key) const;
    inline TUnboxedValue Lookup(const TUnboxedValuePod& key) const;
    inline bool HasDictItems() const;

    // Tuple or Struct accessors
    inline TUnboxedValue GetElement(ui32 index) const;
    inline const TUnboxedValue* GetElements() const;

    // Callable accessors
    inline TUnboxedValue Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const;

    // Resource accessors
    inline TStringRef GetResourceTag() const;
    inline void* GetResource() const;

    inline ui32 GetVariantIndex() const;
    inline TUnboxedValue GetVariantItem() const;

    inline EFetchStatus Fetch(TUnboxedValue& result) const;

    // Any iterator.
    inline bool Skip() const;
    // List iterator.
    inline bool Next(TUnboxedValue& value) const;
    // Dict iterator.
    inline bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) const;

    inline void Apply(IApplyContext& context) const;

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
    // Save/Load state
    inline ui32 GetTraverseCount() const;
    inline TUnboxedValue GetTraverseItem(ui32 index) const;
    inline TUnboxedValue Save() const;
    inline void Load(const TStringRef& state);
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
    inline bool IsSortedDict() const;
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
    inline EFetchStatus WideFetch(TUnboxedValue *result, ui32 width) const;
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
    inline bool Load2(const TUnboxedValue& value);
#endif

    inline bool TryMakeVariant(ui32 index);

    inline void SetTimezoneId(ui16 id);
    inline ui16 GetTimezoneId() const;

protected:
    union TRaw {
        ui64 Halfs[2] = {0, 0};

        TRawEmbeddedValue Embedded;
        
        TRawBoxedValue Boxed;
        
        TRawStringValue String;

        struct {
            union {
                #define FIELD(type) type type##_;
                PRIMITIVE_VALUE_TYPES(FIELD);
                #undef FIELD
                const void* Void;
                ui64 Count;
            };
            union {
                ui64 FullMeta;
                struct {
                    ui16 TimezoneId;
                    ui8 Reserved[4];
                    ui8 Size;
                    ui8 Meta;
                };
            };
        } Simple;

        EMarkers GetMarkers() const {
            return static_cast<EMarkers>(0x3 & Simple.Meta);
        }

        ui8 GetIndex() const {
            return Simple.Meta >> 2;
        }

        explicit operator bool() const { return Simple.FullMeta | Simple.Count; }
    } Raw;

public:
    inline void Ref() const noexcept;
    inline void UnRef() const noexcept;
    inline void ReleaseRef() const noexcept;
    inline void DeleteUnreferenced() const noexcept;
    inline i32 LockRef() const noexcept;
    inline void UnlockRef(i32 prev) const noexcept;
    inline i32 RefCount() const noexcept;

    static constexpr ui32 InternalBufferSize = sizeof(TRaw::Embedded.Buffer);
    static constexpr ui32 OffsetLimit = TRawStringValue::OffsetLimit;
};

UDF_ASSERT_TYPE_SIZE(TUnboxedValuePod, 16);

static_assert(std::is_trivially_destructible<TUnboxedValuePod>::value, "Incompatible with LLVM codegeneration!");
static_assert(std::is_trivially_copy_assignable<TUnboxedValuePod>::value, "Incompatible with LLVM codegeneration!");
static_assert(std::is_trivially_move_assignable<TUnboxedValuePod>::value, "Incompatible with LLVM codegeneration!");
static_assert(std::is_trivially_copy_constructible<TUnboxedValuePod>::value, "Incompatible with LLVM codegeneration!");
static_assert(std::is_trivially_move_constructible<TUnboxedValuePod>::value, "Incompatible with LLVM codegeneration!");

//////////////////////////////////////////////////////////////////////////////
// TUnboxedValue
///////////////////////////////////////////////////////////////////////////////
class TUnboxedValue : public TUnboxedValuePod
{
public:
    inline TUnboxedValue() noexcept = default;
    inline ~TUnboxedValue() noexcept;

    inline TUnboxedValue(const TUnboxedValuePod& value) noexcept;
    inline TUnboxedValue(TUnboxedValuePod&& value) noexcept;

    inline TUnboxedValue(const TUnboxedValue& value) noexcept;
    inline TUnboxedValue(TUnboxedValue&& value) noexcept;

    inline TUnboxedValue& operator=(const TUnboxedValue& value) noexcept;
    inline TUnboxedValue& operator=(TUnboxedValue&& value) noexcept;

    inline TUnboxedValuePod Release() noexcept;

    inline void Clear() noexcept;

    using TAllocator = TStdAllocatorForUdf<TUnboxedValue>;
};

UDF_ASSERT_TYPE_SIZE(TUnboxedValue, 16);

///////////////////////////////////////////////////////////////////////////////
// TBoxedResource
///////////////////////////////////////////////////////////////////////////////
template <typename TResourceData, const char* ResourceTag>
class TBoxedResource: public TBoxedValue
{
public:
    template <typename... Args>
    inline TBoxedResource(Args&&... args)
        : ResourceData_(std::forward<Args>(args)...)
    {
    }

    inline TStringRef GetResourceTag() const override {
        return TStringRef(ResourceTag, std::strlen(ResourceTag));
    }

    inline void* GetResource() override {
        return Get();
    }

    inline TResourceData* Get() {
        return &ResourceData_;
    }

    inline static void Validate(const TUnboxedValuePod& value) {
        Y_DEBUG_ABORT_UNLESS(value.GetResourceTag() == TStringRef(ResourceTag, std::strlen(ResourceTag)));
    }

private:
    TResourceData ResourceData_;
};

#define INCLUDE_UDF_VALUE_INL_H
#include "udf_value_inl.h"
#undef INCLUDE_UDF_VALUE_INL_H

} // namespace NUdf
} // namespace NYql

template<>
inline void Out<NYql::NUdf::TUnboxedValuePod>(class IOutputStream &o, const NYql::NUdf::TUnboxedValuePod& value);

template<>
inline void Out<NYql::NUdf::TUnboxedValue>(class IOutputStream &o, const NYql::NUdf::TUnboxedValue& value);

template<>
inline void Out<NYql::NUdf::EFetchStatus>(class IOutputStream &o, NYql::NUdf::EFetchStatus value);

template<>
inline void Out<NYql::NUdf::TStringRef>(class IOutputStream &o, const NYql::NUdf::TStringRef& value);

#include "udf_terminator.h"
#include <util/stream/output.h>
#include <tuple>

namespace NYql {
namespace NUdf {

//////////////////////////////////////////////////////////////////////////////
// TBoxedValue
//////////////////////////////////////////////////////////////////////////////
inline bool TBoxedValueBase::HasFastListLength() const
{
    Y_ABORT("Not implemented");
}

inline ui64 TBoxedValueBase::GetListLength() const
{
    Y_ABORT("Not implemented");
}

inline ui64 TBoxedValueBase::GetEstimatedListLength() const
{
    Y_ABORT("Not implemented");
}

inline const TOpaqueListRepresentation* TBoxedValueBase::GetListRepresentation() const {
    return nullptr;
}

inline IBoxedValuePtr TBoxedValueBase::ReverseListImpl(const IValueBuilder& builder) const {
    Y_UNUSED(builder);
    return nullptr;
}

inline IBoxedValuePtr TBoxedValueBase::SkipListImpl(const IValueBuilder& builder, ui64 count) const {
    Y_UNUSED(builder);
    Y_UNUSED(count);
    return nullptr;
}

inline IBoxedValuePtr TBoxedValueBase::TakeListImpl(const IValueBuilder& builder, ui64 count) const {
    Y_UNUSED(builder);
    Y_UNUSED(count);
    return nullptr;
}

inline IBoxedValuePtr TBoxedValueBase::ToIndexDictImpl(const IValueBuilder& builder) const {
    Y_UNUSED(builder);
    return nullptr;
}

inline ui64 TBoxedValueBase::GetDictLength() const
{
    Y_ABORT("Not implemented");
}

inline TBoxedValue::TBoxedValue()
{
    UdfRegisterObject(this);
}

inline TBoxedValue::~TBoxedValue()
{
    UdfUnregisterObject(this);
}

inline void TBoxedValueLink::Link(TBoxedValueLink* root) {
    Left = root;
    Right = root->Right;
    Right->Left = Left->Right = this;
}

inline void TBoxedValueLink::Unlink() {
    std::tie(Right->Left, Left->Right) = std::make_pair(Left, Right);
    Left = Right = nullptr;
}

inline TUnboxedValue TBoxedValueBase::GetDictIterator() const
{
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::GetListIterator() const
{
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::GetKeysIterator() const
{
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::GetPayloadsIterator() const
{
    Y_ABORT("Not implemented");
}

inline bool TBoxedValueBase::Skip()
{
    TUnboxedValue stub;
    return Next(stub);
}

inline bool TBoxedValueBase::Next(TUnboxedValue&)
{
    Y_ABORT("Not implemented");
}

inline bool TBoxedValueBase::NextPair(TUnboxedValue&, TUnboxedValue&)
{
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::GetElement(ui32 index) const
{
    Y_UNUSED(index);
    Y_ABORT("Not implemented");
}

inline const TUnboxedValue* TBoxedValueBase::GetElements() const
{
    return nullptr;
}

inline void TBoxedValueBase::Apply(IApplyContext&) const
{
    Y_ABORT("Not implemented");
}

inline TStringRef TBoxedValueBase::GetResourceTag() const {
    Y_ABORT("Not implemented");
}

inline void* TBoxedValueBase::GetResource()
{
    Y_ABORT("Not implemented");
}

inline bool TBoxedValueBase::HasListItems() const {
    Y_ABORT("Not implemented");
}

inline bool TBoxedValueBase::HasDictItems() const {
    Y_ABORT("Not implemented");
}

inline ui32 TBoxedValueBase::GetVariantIndex() const {
    Y_ABORT("Not implemented");
}

inline bool TBoxedValueBase::Contains(const TUnboxedValuePod& key) const
{
    Y_UNUSED(key);
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::Lookup(const TUnboxedValuePod& key) const
{
    Y_UNUSED(key);
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::Run(const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const
{
    Y_UNUSED(valueBuilder);
    Y_UNUSED(args);
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::GetVariantItem() const {
    Y_ABORT("Not implemented");
}

inline EFetchStatus TBoxedValueBase::Fetch(TUnboxedValue& result) {
    Y_UNUSED(result);
    Y_ABORT("Not implemented");
}

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
inline ui32 TBoxedValueBase::GetTraverseCount() const {
    Y_ABORT("Not implemented");
    return 0;
}

inline TUnboxedValue TBoxedValueBase::GetTraverseItem(ui32 index) const {
    Y_UNUSED(index);
    Y_ABORT("Not implemented");
}

inline TUnboxedValue TBoxedValueBase::Save() const {
    Y_ABORT("Not implemented");
}

inline void TBoxedValueBase::Load(const TStringRef& state) {
    Y_UNUSED(state);
    Y_ABORT("Not implemented");
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
inline void TBoxedValueBase::Push(const TUnboxedValuePod& value) {
    Y_UNUSED(value);
    Y_ABORT("Not implemented");
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
inline bool TBoxedValueBase::IsSortedDict() const {
    Y_ABORT("Not implemented");
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
inline void TBoxedValueBase::Unused1() {
    Y_ABORT("Not implemented");
}

inline void TBoxedValueBase::Unused2() {
    Y_ABORT("Not implemented");
}

inline void TBoxedValueBase::Unused3() {
    Y_ABORT("Not implemented");
}

inline void TBoxedValueBase::Unused4() {
    Y_ABORT("Not implemented");
}

inline void TBoxedValueBase::Unused5() {
    Y_ABORT("Not implemented");
}

inline void TBoxedValueBase::Unused6() {
    Y_ABORT("Not implemented");
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
inline EFetchStatus TBoxedValueBase::WideFetch(TUnboxedValue *result, ui32 width) {
    Y_UNUSED(result);
    Y_UNUSED(width);
    Y_ABORT("Not implemented");
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
inline bool TBoxedValueBase::Load2(const TUnboxedValue& value) {
    Y_UNUSED(value);
    Y_ABORT("Not implemented");
}
#endif

inline void TUnboxedValuePod::Dump(IOutputStream& out) const {
    switch (Raw.GetMarkers()) {
    case EMarkers::Empty:
        out << "Empty, count: " << (i64)Raw.Simple.Count;
        break;
    case EMarkers::Embedded:
        out << "Embedded, size: " << (ui32)Raw.Embedded.Size << ", buffer: " <<
            TString(Raw.Embedded.Buffer, sizeof(Raw.Embedded.Buffer)).Quote();
        break;
    case EMarkers::String: {
        out << "String, size: " << Raw.String.Size << ", offset: " << (Raw.String.Offset & 0xffffff) << ", buffer: ";
        auto ref = AsStringRef();
        out << TString(ref.Data(), ref.Size()).Quote();
        break;
    }
    case EMarkers::Boxed:
        out << "Boxed, pointer: " << (void*)Raw.Boxed.Value;
        break;
    }
}

} // namespace NUdf
} // namespace NYql

template<>
inline void Out<NYql::NUdf::TUnboxedValuePod>(class IOutputStream &o, const NYql::NUdf::TUnboxedValuePod& value) {
    value.Dump(o);
}

template<>
inline void Out<NYql::NUdf::TUnboxedValue>(class IOutputStream &o, const NYql::NUdf::TUnboxedValue& value) {
    value.Dump(o);
}

template<>
inline void Out<NYql::NUdf::EFetchStatus>(class IOutputStream &o, NYql::NUdf::EFetchStatus value) {
    switch (value) {
    case NYql::NUdf::EFetchStatus::Ok:
        o << "Ok";
        break;
    case NYql::NUdf::EFetchStatus::Yield:
        o << "Yield";
        break;
    case NYql::NUdf::EFetchStatus::Finish:
        o << "Finish";
        break;
    }
}

template<>
inline void Out<NYql::NUdf::TStringRef>(class IOutputStream &o, const NYql::NUdf::TStringRef& value) {
    o << TStringBuf(value.Data(), value.Size());
}

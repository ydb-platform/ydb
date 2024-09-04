#pragma once

#if !defined(INCLUDE_UDF_VALUE_INL_H)
#error "you should never include udf_value_inl.h directly"
#endif  // INCLUDE_UDF_VALUE_INL_H

#ifdef LLVM_BC

#define UDF_VERIFY(expr, ...)                     \
    do {                                          \
        if (false) {                              \
            bool __xxx = static_cast<bool>(expr); \
            Y_UNUSED(__xxx);                      \
        }                                         \
    } while (false)

#define UDF_ALWAYS_INLINE   __attribute__((always_inline))

#else

#define UDF_VERIFY Y_DEBUG_ABORT_UNLESS
#define UDF_ALWAYS_INLINE   Y_FORCE_INLINE

#endif

//////////////////////////////////////////////////////////////////////////////
// IBoxedValue
//////////////////////////////////////////////////////////////////////////////
inline void IBoxedValue1::Ref() noexcept
{
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
    if (Refs_ < 0)
        return;
#endif
    ++Refs_;
}

inline void IBoxedValue1::UnRef() noexcept
{
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
    if (Refs_ < 0)
        return;
#endif
    Y_DEBUG_ABORT_UNLESS(Refs_ > 0);
    if (!--Refs_)
        delete this;
}

inline void IBoxedValue1::ReleaseRef() noexcept
{
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 4)
    if (Refs_ < 0)
        return;
#endif
    Y_DEBUG_ABORT_UNLESS(Refs_ > 0);
    --Refs_;
}

inline void IBoxedValue1::DeleteUnreferenced() noexcept
{
    if (!Refs_)
        delete this;
}

inline i32 IBoxedValue1::RefCount() const noexcept
{
    return Refs_;
}

inline void IBoxedValue1::SetUserMark(ui8 mark) noexcept {
    UserMark_ = mark;
}

inline ui8 IBoxedValue1::UserMark() const noexcept {
    return UserMark_;
}

inline i32 IBoxedValue1::LockRef() noexcept {
   Y_DEBUG_ABORT_UNLESS(Refs_ != -1);
   auto ret = Refs_;
   Refs_ = -1;
   return ret;
}

inline void IBoxedValue1::UnlockRef(i32 prev) noexcept {
   Y_DEBUG_ABORT_UNLESS(Refs_ == -1);
   Y_DEBUG_ABORT_UNLESS(prev != -1);
   Refs_ = prev;
}

//////////////////////////////////////////////////////////////////////////////
// TBoxedValueAccessor
//////////////////////////////////////////////////////////////////////////////

inline bool TBoxedValueAccessor::HasFastListLength(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.HasFastListLength();
}

inline ui64 TBoxedValueAccessor::GetListLength(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetListLength();
}

inline ui64 TBoxedValueAccessor::GetEstimatedListLength(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetEstimatedListLength();
}

inline TUnboxedValue TBoxedValueAccessor::GetListIterator(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetListIterator();
}

inline const TOpaqueListRepresentation* TBoxedValueAccessor::GetListRepresentation(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetListRepresentation();
}

inline IBoxedValuePtr TBoxedValueAccessor::ReverseListImpl(const IBoxedValue& value, const IValueBuilder& builder) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.ReverseListImpl(builder);
}

inline IBoxedValuePtr TBoxedValueAccessor::SkipListImpl(const IBoxedValue& value, const IValueBuilder& builder, ui64 count) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.SkipListImpl(builder, count);
}

inline IBoxedValuePtr TBoxedValueAccessor::TakeListImpl(const IBoxedValue& value, const IValueBuilder& builder, ui64 count) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.TakeListImpl(builder, count);
}

inline IBoxedValuePtr TBoxedValueAccessor::ToIndexDictImpl(const IBoxedValue& value, const IValueBuilder& builder) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.ToIndexDictImpl(builder);
}

inline ui64 TBoxedValueAccessor::GetDictLength(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetDictLength();
}

inline TUnboxedValue TBoxedValueAccessor::GetDictIterator(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetDictIterator();
}

inline TUnboxedValue TBoxedValueAccessor::GetKeysIterator(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetKeysIterator();
}

inline TUnboxedValue TBoxedValueAccessor::GetPayloadsIterator(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetPayloadsIterator();
}

inline bool TBoxedValueAccessor::Contains(const IBoxedValue& value, const TUnboxedValuePod& key) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.Contains(key);
}

inline TUnboxedValue TBoxedValueAccessor::Lookup(const IBoxedValue& value, const TUnboxedValuePod& key) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.Lookup(key);
}

inline TUnboxedValue TBoxedValueAccessor::GetElement(const IBoxedValue& value, ui32 index) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetElement(index);
}

inline const TUnboxedValue* TBoxedValueAccessor::GetElements(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetElements();
}

inline TUnboxedValue TBoxedValueAccessor::Run(const IBoxedValue& value, const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.Run(valueBuilder, args);
}

inline TStringRef TBoxedValueAccessor::GetResourceTag(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetResourceTag();
}

inline void* TBoxedValueAccessor::GetResource(IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.GetResource();
}

inline bool TBoxedValueAccessor::HasListItems(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.HasListItems();
}

inline bool TBoxedValueAccessor::HasDictItems(const IBoxedValue& value) {
   Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
   return value.HasDictItems();
}

inline ui32 TBoxedValueAccessor::GetVariantIndex(const IBoxedValue& value) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
    return value.GetVariantIndex();
}

inline TUnboxedValue TBoxedValueAccessor::GetVariantItem(const IBoxedValue& value) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
    return value.GetVariantItem();
}

inline EFetchStatus TBoxedValueAccessor::Fetch(IBoxedValue& value, TUnboxedValue& result) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
    return value.Fetch(result);
}

inline bool TBoxedValueAccessor::Skip(IBoxedValue& value) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
    return value.Skip();
}

inline bool TBoxedValueAccessor::Next(IBoxedValue& value, TUnboxedValue& result) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
    return value.Next(result);
}

inline bool TBoxedValueAccessor::NextPair(IBoxedValue& value, TUnboxedValue& key, TUnboxedValue& payload) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
    return value.NextPair(key, payload);
}

inline void TBoxedValueAccessor::Apply(IBoxedValue& value, IApplyContext& context) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 0)));
    return value.Apply(context);
}

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
inline ui32 TBoxedValueAccessor::GetTraverseCount(const IBoxedValue& value) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 3)));
    return value.GetTraverseCount();
}

inline TUnboxedValue TBoxedValueAccessor::GetTraverseItem(const IBoxedValue& value, ui32 index) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 3)));
    return value.GetTraverseItem(index);
}

inline TUnboxedValue TBoxedValueAccessor::Save(const IBoxedValue& value) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 3)));
    return value.Save();
}

inline void TBoxedValueAccessor::Load(IBoxedValue& value, const TStringRef& state) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 3)));
    value.Load(state);
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
inline void TBoxedValueAccessor::Push(IBoxedValue& value, const TUnboxedValuePod& data) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 11)));
    return value.Push(data);
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
inline bool TBoxedValueAccessor::IsSortedDict(IBoxedValue& value) {
    if (!value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 12))) {
        return false;
    }

    return value.IsSortedDict();
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
inline EFetchStatus TBoxedValueAccessor::WideFetch(IBoxedValue& value, TUnboxedValue* result, ui32 width) {
    Y_DEBUG_ABORT_UNLESS(value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 30)));
    return value.WideFetch(result, width);
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
inline bool TBoxedValueAccessor::Load2(IBoxedValue& value, const TUnboxedValue& data) {
    if (!value.IsCompatibleTo(MakeAbiCompatibilityVersion(2, 36))) {
        return false;
    }

    return value.Load2(data);
}
#endif

//////////////////////////////////////////////////////////////////////////////
// TUnboxedValue
//////////////////////////////////////////////////////////////////////////////
Y_FORCE_INLINE TUnboxedValue::TUnboxedValue(const TUnboxedValuePod& value) noexcept
    : TUnboxedValuePod(value)
{
    Ref();
}

Y_FORCE_INLINE TUnboxedValue::TUnboxedValue(TUnboxedValuePod&& value) noexcept
    : TUnboxedValuePod(std::move(value))
{
    value.Raw = TRaw();
    Ref();
}

Y_FORCE_INLINE TUnboxedValue::TUnboxedValue(const TUnboxedValue& value) noexcept
    : TUnboxedValuePod(static_cast<const TUnboxedValuePod&>(value))
{
    Ref();
}

Y_FORCE_INLINE TUnboxedValue::TUnboxedValue(TUnboxedValue&& value) noexcept
    : TUnboxedValuePod(static_cast<TUnboxedValuePod&&>(value))
{
    value.Raw = TRaw();
}

Y_FORCE_INLINE TUnboxedValue& TUnboxedValue::operator=(const TUnboxedValue& value) noexcept
{
    if (this != &value) {
        value.Ref();
        UnRef();
        Raw = value.Raw;
    }
    return *this;
}

Y_FORCE_INLINE TUnboxedValue& TUnboxedValue::operator=(TUnboxedValue&& value) noexcept
{
    if (this != &value) {
        UnRef();
        Raw = value.Raw;
        value.Raw = TRaw();
    }
    return *this;
}

Y_FORCE_INLINE TUnboxedValuePod TUnboxedValue::Release() noexcept {
    const TUnboxedValuePod value(std::move(*static_cast<TUnboxedValuePod*>(this)));
    Raw = TRaw();
    value.ReleaseRef();
    return value;
}

Y_FORCE_INLINE void TUnboxedValue::Clear() noexcept
{
    UnRef();
    Raw = TRaw();
}

Y_FORCE_INLINE TUnboxedValue::~TUnboxedValue() noexcept
{
    UnRef();
}
//////////////////////////////////////////////////////////////////////////////
// TUnboxedValuePod
//////////////////////////////////////////////////////////////////////////////
Y_FORCE_INLINE TUnboxedValuePod::TUnboxedValuePod(IBoxedValuePtr&& value)
{
    Raw.Boxed.Meta = static_cast<ui8>(EMarkers::Boxed);
    Raw.Boxed.Value = value.Release();
    Raw.Boxed.Value->ReleaseRef();
}

Y_FORCE_INLINE TUnboxedValuePod::TUnboxedValuePod(TStringValue&& value, ui32 size, ui32 offset)
{
    Y_DEBUG_ABORT_UNLESS(size);
    Y_DEBUG_ABORT_UNLESS(offset < std::min(OffsetLimit, value.Size()));
    Raw.String.Size = std::min(value.Size() - offset, size);
    Raw.String.Offset = offset;
    Raw.String.Value = value.ReleaseBuf();
    Raw.String.Meta = static_cast<ui8>(EMarkers::String);
}

inline TStringValue TUnboxedValuePod::AsStringValue() const
{
    UDF_VERIFY(IsString(), "Value is not a string");
    return TStringValue(Raw.String.Value);
}

inline IBoxedValuePtr TUnboxedValuePod::AsBoxed() const
{
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return IBoxedValuePtr(Raw.Boxed.Value);
}

inline TStringValue::TData* TUnboxedValuePod::AsRawStringValue() const
{
    UDF_VERIFY(IsString(), "Value is not a string");
    return Raw.String.Value;
}

inline IBoxedValue* TUnboxedValuePod::AsRawBoxed() const
{
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return Raw.Boxed.Value;
}

inline bool TUnboxedValuePod::UniqueBoxed() const
{
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return Raw.Boxed.Value->RefCount() <= 1;
}

inline bool TUnboxedValuePod::HasFastListLength() const {
    UDF_VERIFY(IsBoxed(), "Value is not a list");
    return TBoxedValueAccessor::HasFastListLength(*Raw.Boxed.Value);
}

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 11)
inline void TUnboxedValuePod::Push(const TUnboxedValuePod& value) const {
    UDF_VERIFY(IsBoxed(), "Value is not a list");
    return TBoxedValueAccessor::Push(*Raw.Boxed.Value, value);
}
#endif

inline ui64 TUnboxedValuePod::GetListLength() const
{
    UDF_VERIFY(IsBoxed(), "Value is not a list");
    return TBoxedValueAccessor::GetListLength(*Raw.Boxed.Value);
}

inline ui64 TUnboxedValuePod::GetEstimatedListLength() const {
    UDF_VERIFY(IsBoxed(), "Value is not a list");
    return TBoxedValueAccessor::GetEstimatedListLength(*Raw.Boxed.Value);
}

inline bool TUnboxedValuePod::HasListItems() const {
    UDF_VERIFY(IsBoxed(), "Value is not a list");
    return TBoxedValueAccessor::HasListItems(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::GetListIterator() const
{
    UDF_VERIFY(IsBoxed(), "Value is not a list");
    return TBoxedValueAccessor::GetListIterator(*Raw.Boxed.Value);
}

inline TUnboxedValuePod TUnboxedValuePod::MakeOptional() const
{
    if (Raw.Simple.Meta)
        return *this;

    TUnboxedValuePod result(*this);
    ++result.Raw.Simple.Count;
    return result;
}

inline TUnboxedValuePod TUnboxedValuePod::GetOptionalValue() const
{
    if (Raw.Simple.Meta)
        return *this;

    UDF_VERIFY(Raw.Simple.Count > 0U, "Can't get value from empty.");

    TUnboxedValuePod result(*this);
    --result.Raw.Simple.Count;
    return result;
}

template<> inline TUnboxedValuePod TUnboxedValuePod::GetOptionalValueIf<false>() const { return *this; }
template<> inline TUnboxedValuePod TUnboxedValuePod::GetOptionalValueIf<true>() const { return GetOptionalValue(); }

template<> inline TUnboxedValuePod TUnboxedValuePod::MakeOptionalIf<false>() const { return *this; }
template<> inline TUnboxedValuePod TUnboxedValuePod::MakeOptionalIf<true>() const { return MakeOptional(); }

inline ui64 TUnboxedValuePod::GetDictLength() const
{
    UDF_VERIFY(IsBoxed(), "Value is not a dict");
    return TBoxedValueAccessor::GetDictLength(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::GetDictIterator() const
{
    UDF_VERIFY(IsBoxed(), "Value is not a dict");
    return TBoxedValueAccessor::GetDictIterator(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::GetKeysIterator() const
{
    UDF_VERIFY(IsBoxed(), "Value is not a dict");
    return TBoxedValueAccessor::GetKeysIterator(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::GetPayloadsIterator() const
{
    UDF_VERIFY(IsBoxed(), "Value is not a dict");
    return TBoxedValueAccessor::GetPayloadsIterator(*Raw.Boxed.Value);
}

inline bool TUnboxedValuePod::Contains(const TUnboxedValuePod& key) const
{
    UDF_VERIFY(IsBoxed(), "Value is not a dict");
    return TBoxedValueAccessor::Contains(*Raw.Boxed.Value, key);
}

inline TUnboxedValue TUnboxedValuePod::Lookup(const TUnboxedValuePod& key) const
{
    UDF_VERIFY(IsBoxed(), "Value is not a dict");
    return TBoxedValueAccessor::Lookup(*Raw.Boxed.Value, key);
}

inline bool TUnboxedValuePod::HasDictItems() const {
    UDF_VERIFY(IsBoxed(), "Value is not a dict");
    return TBoxedValueAccessor::HasDictItems(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::GetElement(ui32 index) const
{
    UDF_VERIFY(IsBoxed(), "Value is not a tuple");
    return TBoxedValueAccessor::GetElement(*Raw.Boxed.Value, index);
}

inline const TUnboxedValue* TUnboxedValuePod::GetElements() const
{
    UDF_VERIFY(IsBoxed(), "Value is not a tuple");
    return TBoxedValueAccessor::GetElements(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::Run(
        const IValueBuilder* valueBuilder, const TUnboxedValuePod* args) const
{
    UDF_VERIFY(IsBoxed(), "Value is not a callable");
    return TBoxedValueAccessor::Run(*Raw.Boxed.Value, valueBuilder, args);
}

inline TStringRef TUnboxedValuePod::GetResourceTag() const {
    UDF_VERIFY(IsBoxed(), "Value is not a resource");
    return TBoxedValueAccessor::GetResourceTag(*Raw.Boxed.Value);
}

inline void* TUnboxedValuePod::GetResource() const {
    UDF_VERIFY(IsBoxed(), "Value is not a resource");
    return TBoxedValueAccessor::GetResource(*Raw.Boxed.Value);
}

inline ui32 TUnboxedValuePod::GetVariantIndex() const {
    if (auto index = Raw.GetIndex())
        return --index;
    UDF_VERIFY(IsBoxed(), "Value is not a variant");
    return TBoxedValueAccessor::GetVariantIndex(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::GetVariantItem() const {
    if (Raw.GetIndex()) {
        TUnboxedValuePod item(*this);
        item.Raw.Simple.Meta &= 0x3;
        return std::move(item);
    }
    UDF_VERIFY(IsBoxed(), "Value is not a variant");
    return TBoxedValueAccessor::GetVariantItem(*Raw.Boxed.Value);
}

inline bool TUnboxedValuePod::TryMakeVariant(ui32 index) {
    static const ui32 limit = (1U << 6U) - 1U;
    if (index >= limit || Raw.GetIndex())
        return false;

    Raw.Simple.Meta |= ui8(++index << 2);
    return true;
}

inline void TUnboxedValuePod::SetTimezoneId(ui16 id) {
    UDF_VERIFY(IsEmbedded(), "Value is not a datetime");
    Raw.Simple.TimezoneId = id;
}

inline ui16 TUnboxedValuePod::GetTimezoneId() const {
    UDF_VERIFY(IsEmbedded(), "Value is not a datetime");
    return Raw.Simple.TimezoneId;
}

inline EFetchStatus TUnboxedValuePod::Fetch(TUnboxedValue& result) const {
    UDF_VERIFY(IsBoxed(), "Value is not a stream");
    return TBoxedValueAccessor::Fetch(*Raw.Boxed.Value, result);
}

inline bool TUnboxedValuePod::Skip() const {
    UDF_VERIFY(IsBoxed(), "Value is not a iterator");
    return TBoxedValueAccessor::Skip(*Raw.Boxed.Value);
}

inline bool TUnboxedValuePod::Next(TUnboxedValue& value) const {
    UDF_VERIFY(IsBoxed(), "Value is not a iterator");
    return TBoxedValueAccessor::Next(*Raw.Boxed.Value, value);
}

inline bool TUnboxedValuePod::NextPair(TUnboxedValue& key, TUnboxedValue& payload) const {
    UDF_VERIFY(IsBoxed(), "Value is not a iterator");
    return TBoxedValueAccessor::NextPair(*Raw.Boxed.Value, key, payload);
}

inline void TUnboxedValuePod::Apply(IApplyContext& context) const {
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return TBoxedValueAccessor::Apply(*Raw.Boxed.Value, context);
}

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 3)
inline ui32 TUnboxedValuePod::GetTraverseCount() const {
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return TBoxedValueAccessor::GetTraverseCount(*Raw.Boxed.Value);
}

inline TUnboxedValue TUnboxedValuePod::GetTraverseItem(ui32 index) const {
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return TBoxedValueAccessor::GetTraverseItem(*Raw.Boxed.Value, index);
}

inline TUnboxedValue TUnboxedValuePod::Save() const {
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return TBoxedValueAccessor::Save(*Raw.Boxed.Value);
}

inline void TUnboxedValuePod::Load(const TStringRef& state) {
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return TBoxedValueAccessor::Load(*Raw.Boxed.Value, state);
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 12)
inline bool TUnboxedValuePod::IsSortedDict() const {
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return TBoxedValueAccessor::IsSortedDict(*Raw.Boxed.Value);
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 30)
inline EFetchStatus TUnboxedValuePod::WideFetch(TUnboxedValue* result, ui32 width) const {
    UDF_VERIFY(IsBoxed(), "Value is not a wide stream");
    return TBoxedValueAccessor::WideFetch(*Raw.Boxed.Value, result, width);
}
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 36)
inline bool TUnboxedValuePod::Load2(const TUnboxedValue& value) {
    UDF_VERIFY(IsBoxed(), "Value is not boxed");
    return TBoxedValueAccessor::Load2(*Raw.Boxed.Value, value);
}
#endif

Y_FORCE_INLINE void TUnboxedValuePod::Ref() const noexcept
{
    switch (Raw.GetMarkers()) {
    case EMarkers::String: return Raw.String.Value->Ref();
    case EMarkers::Boxed: return Raw.Boxed.Value->Ref();
    default: return;
    }
}

Y_FORCE_INLINE void TUnboxedValuePod::UnRef() const noexcept
{
    switch (Raw.GetMarkers()) {
    case EMarkers::String: return Raw.String.Value->UnRef();
    case EMarkers::Boxed: return Raw.Boxed.Value->UnRef();
    default: return;
    }
}

Y_FORCE_INLINE void TUnboxedValuePod::ReleaseRef() const noexcept
{
    switch (Raw.GetMarkers()) {
    case EMarkers::String: return Raw.String.Value->ReleaseRef();
    case EMarkers::Boxed: return Raw.Boxed.Value->ReleaseRef();
    default: return;
    }
}

Y_FORCE_INLINE void TUnboxedValuePod::DeleteUnreferenced() const noexcept
{
    switch (Raw.GetMarkers()) {
    case EMarkers::String: return Raw.String.Value->DeleteUnreferenced();
    case EMarkers::Boxed: return Raw.Boxed.Value->DeleteUnreferenced();
    default: return;
    }
}

Y_FORCE_INLINE i32 TUnboxedValuePod::LockRef() const noexcept
{
    switch (Raw.GetMarkers()) {
    case EMarkers::String: return Raw.String.Value->LockRef();
    case EMarkers::Boxed: return Raw.Boxed.Value->LockRef();
    default: return -1;
    }
}

Y_FORCE_INLINE void TUnboxedValuePod::UnlockRef(i32 prev) const noexcept
{
    switch (Raw.GetMarkers()) {
    case EMarkers::String: return Raw.String.Value->UnlockRef(prev);
    case EMarkers::Boxed: return Raw.Boxed.Value->UnlockRef(prev);
    default: return;
    }
}

Y_FORCE_INLINE i32 TUnboxedValuePod::RefCount() const noexcept
{
    switch (Raw.GetMarkers()) {
    case EMarkers::String: return Raw.String.Value->RefCount();
    case EMarkers::Boxed: return Raw.Boxed.Value->RefCount();
    default: return -1;
    }
}

#define VALUE_GET(xType) \
    template <> \
    inline xType TUnboxedValuePod::Get<xType>() const \
    { \
        UDF_VERIFY(EMarkers::Embedded == Raw.GetMarkers(), "Value is empty."); \
        return Raw.Simple.xType##_; \
    }

#define VALUE_GET_DEF(xType) \
    template <> \
    inline xType TUnboxedValuePod::GetOrDefault<xType>(xType def) const \
    { \
        return EMarkers::Empty == Raw.GetMarkers() ? def : Raw.Simple.xType##_; \
    }

#define VALUE_CONSTR(xType) \
    template <> \
    inline TUnboxedValuePod::TUnboxedValuePod(xType value) \
    { \
        Raw.Simple.xType##_ = value; \
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded); \
    }

PRIMITIVE_VALUE_TYPES(VALUE_GET)
PRIMITIVE_VALUE_TYPES(VALUE_GET_DEF)
PRIMITIVE_VALUE_TYPES(VALUE_CONSTR)

#undef VALUE_GET
#undef VALUE_GET_DEF
#undef VALUE_CONSTR

template <>
inline bool TUnboxedValuePod::Get<bool>() const
{
    UDF_VERIFY(EMarkers::Empty != Raw.GetMarkers(), "Value is empty.");
    return bool(Raw.Simple.ui8_);
}

template <>
inline bool TUnboxedValuePod::GetOrDefault<bool>(bool def) const
{
    return EMarkers::Empty == Raw.GetMarkers() ? def : bool(Raw.Simple.ui8_);
}

template <>
inline NYql::NDecimal::TInt128 TUnboxedValuePod::Get<NYql::NDecimal::TInt128>() const
{
    return GetInt128();
}

template <>
inline TUnboxedValuePod::TUnboxedValuePod(bool value)
{
    Raw.Simple.ui8_ = value ? 1 : 0;
    Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded);
}

inline NYql::NDecimal::TInt128 TUnboxedValuePod::GetInt128() const
{
    UDF_VERIFY(EMarkers::Empty != Raw.GetMarkers(), "Value is empty.");
    auto v = *reinterpret_cast<const NYql::NDecimal::TInt128*>(&Raw);
    const auto p = reinterpret_cast<ui8*>(&v);
    p[0xF] = (p[0xE] & 0x80) ? 0xFF : 0x00;
    return v;
}

inline NYql::NDecimal::TUint128 TUnboxedValuePod::GetUint128() const
{
    UDF_VERIFY(EMarkers::Empty != Raw.GetMarkers(), "Value is empty.");
    auto v = *reinterpret_cast<const NYql::NDecimal::TUint128*>(&Raw);
    const auto p = reinterpret_cast<ui8*>(&v);
    p[0xF] = (p[0xE] & 0x80) ? 0xFF : 0x00;
    return v;
}

inline TUnboxedValuePod::TUnboxedValuePod(NYql::NDecimal::TInt128 value)
{
    *reinterpret_cast<NYql::NDecimal::TInt128*>(&Raw) = value;
    Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded);
}

inline TUnboxedValuePod::TUnboxedValuePod(NYql::NDecimal::TUint128 value)
{
    *reinterpret_cast<NYql::NDecimal::TUint128*>(&Raw) = value;
    Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded);
}

inline const void* TUnboxedValuePod::GetRawPtr() const
{
    return &Raw;
}

inline void* TUnboxedValuePod::GetRawPtr()
{
    return &Raw;
}

inline TUnboxedValuePod TUnboxedValuePod::Void()
{
    TUnboxedValuePod v;
    v.Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded);
    return v;
}

inline TUnboxedValuePod TUnboxedValuePod::Zero()
{
    TUnboxedValuePod v;
    v.Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded);
    return v;
}

inline TUnboxedValuePod TUnboxedValuePod::Embedded(ui8 size) {
    UDF_VERIFY(size <= InternalBufferSize);

    TUnboxedValuePod v;
    v.Raw.Embedded.Size = size;
    v.Raw.Embedded.Meta = static_cast<ui8>(EMarkers::Embedded);
    return v;
}

inline TUnboxedValuePod TUnboxedValuePod::Embedded(const TStringRef& value) {
    UDF_VERIFY(value.Size() <= InternalBufferSize);

    TUnboxedValuePod v;
    v.Raw.Embedded.Size = value.Size();
    v.Raw.Embedded.Meta = static_cast<ui8>(EMarkers::Embedded);
    if (v.Raw.Embedded.Size) {
        std::memcpy(v.Raw.Embedded.Buffer, value.Data(), v.Raw.Embedded.Size);
    }

    return v;
}

inline TUnboxedValuePod TUnboxedValuePod::Invalid()
{
    TUnboxedValuePod v;
    v.Raw.Simple.Count = std::numeric_limits<ui64>::max();
    return v;
}

inline TUnboxedValuePod TUnboxedValuePod::MakeFinish()
{
    TUnboxedValuePod v;
    v.Raw.Simple.Count = std::numeric_limits<ui64>::max() - 1U;
    return v;
}

inline TUnboxedValuePod TUnboxedValuePod::MakeYield()
{
    return Invalid();
}

inline bool TUnboxedValuePod::IsInvalid() const
{
    return Raw.Simple.Count == std::numeric_limits<ui64>::max() && Raw.Simple.FullMeta == 0;
}

inline bool TUnboxedValuePod::IsFinish() const
{
    return Raw.Simple.Count == std::numeric_limits<ui64>::max() - 1U && Raw.Simple.FullMeta == 0;
}

inline bool TUnboxedValuePod::IsYield() const
{
    return IsInvalid();
}

inline bool TUnboxedValuePod::IsSpecial() const
{
    return Raw.Simple.FullMeta == 0 && Raw.Simple.Count >= std::numeric_limits<ui64>::max() - 1U;
}

inline TStringRef TUnboxedValuePod::AsStringRef() const&
{
    switch (Raw.GetMarkers()) {
    case EMarkers::Embedded: return { Raw.Embedded.Buffer, Raw.Embedded.Size };
    case EMarkers::String: return { Raw.String.Value->Data() + (Raw.String.Offset & 0xFFFFFF), Raw.String.Size };
    default: Y_ABORT("Value is not a string.");
    }
}

inline TMutableStringRef TUnboxedValuePod::AsStringRef() &
{
    switch (Raw.GetMarkers()) {
    case EMarkers::Embedded: return { Raw.Embedded.Buffer, Raw.Embedded.Size };
    case EMarkers::String: return { Raw.String.Value->Data() + (Raw.String.Offset & 0xFFFFFF), Raw.String.Size };
    default: Y_ABORT("Value is not a string.");
    }
}

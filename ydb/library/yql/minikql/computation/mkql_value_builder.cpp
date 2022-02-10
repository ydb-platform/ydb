#include "mkql_value_builder.h"
#include "mkql_validate.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h> 
#include <ydb/library/yql/minikql/mkql_string_util.h> 
#include <library/cpp/yson/node/node_io.h>

#include <util/system/env.h>

namespace NKikimr {
namespace NMiniKQL {

///////////////////////////////////////////////////////////////////////////////
// TDefaultValueBuilder
///////////////////////////////////////////////////////////////////////////////
TDefaultValueBuilder::TDefaultValueBuilder(const THolderFactory& holderFactory, NUdf::EValidatePolicy policy)
    : HolderFactory_(holderFactory)
    , Policy_(policy)
{}

void TDefaultValueBuilder::SetSecureParamsProvider(const NUdf::ISecureParamsProvider* provider) {
    SecureParamsProvider_ = provider;
}

void TDefaultValueBuilder::RethrowAtTerminate() {
    Rethrow_ = true;
}

void TDefaultValueBuilder::SetCalleePositionHolder(const NUdf::TSourcePosition*& position) {
    CalleePositionPtr_ = &position;
}

void TDefaultValueBuilder::Terminate(const char* message) const {
    TStringBuf reason = (message ? TStringBuf(message) : TStringBuf("(unknown)"));
    TString fullMessage = TStringBuilder() <<
        "Terminate was called, reason(" << reason.size() << "): " << reason << Endl;
    HolderFactory_.CleanupModulesOnTerminate(); 
    if (Policy_ == NUdf::EValidatePolicy::Exception) { 
        if (Rethrow_ && std::current_exception()) { 
            throw; 
        } 
 
        Rethrow_ = true; 
        ythrow yexception() << fullMessage; 
    }
 
    Cerr << fullMessage; 
    abort(); 
} 
 
NUdf::TUnboxedValue TDefaultValueBuilder::NewStringNotFilled(ui32 size) const
{
    return MakeStringNotFilled(size);
}

NUdf::TUnboxedValue TDefaultValueBuilder::NewString(const NUdf::TStringRef& ref) const
{
    return MakeString(ref);
}

NUdf::TUnboxedValue TDefaultValueBuilder::ConcatStrings(NUdf::TUnboxedValuePod first, NUdf::TUnboxedValuePod second) const
{
    return ::NKikimr::NMiniKQL::ConcatStrings(first, second);
}

NUdf::TUnboxedValue TDefaultValueBuilder::AppendString(NUdf::TUnboxedValuePod value, const NUdf::TStringRef& ref) const
{
    return ::NKikimr::NMiniKQL::AppendString(value, ref);
}

NUdf::TUnboxedValue TDefaultValueBuilder::PrependString(const NUdf::TStringRef& ref, NUdf::TUnboxedValuePod value) const
{
    return ::NKikimr::NMiniKQL::PrependString(ref, value);
}

NUdf::TUnboxedValue TDefaultValueBuilder::SubString(NUdf::TUnboxedValuePod value, ui32 offset, ui32 size) const
{
    return ::NKikimr::NMiniKQL::SubString(value, offset, size);
}

NUdf::TUnboxedValue TDefaultValueBuilder::NewList(NUdf::TUnboxedValue* items, ui64 count) const {
    if (!items || !count)
        return HolderFactory_.GetEmptyContainer();

    if (count < Max<ui32>()) {
        NUdf::TUnboxedValue* inplace = nullptr;
        auto array = HolderFactory_.CreateDirectArrayHolder(count, inplace);
        for (ui64 i = 0; i < count; ++i)
            *inplace++ = std::move(*items++);
        return std::move(array);
    }

    TDefaultListRepresentation list;
    for (ui64 i = 0; i < count; ++i) {
        list = list.Append(std::move(*items++));
    }

    return HolderFactory_.CreateDirectListHolder(std::move(list));
}

NUdf::TUnboxedValue TDefaultValueBuilder::ReverseList(const NUdf::TUnboxedValuePod& list) const
{
    return HolderFactory_.ReverseList(this, list);
}

NUdf::TUnboxedValue TDefaultValueBuilder::SkipList(const NUdf::TUnboxedValuePod& list, ui64 count) const
{
    return HolderFactory_.SkipList(this, list, count);
}

NUdf::TUnboxedValue TDefaultValueBuilder::TakeList(const NUdf::TUnboxedValuePod& list, ui64 count) const
{
    return HolderFactory_.TakeList(this, list, count);
}

NUdf::TUnboxedValue TDefaultValueBuilder::ToIndexDict(const NUdf::TUnboxedValuePod& list) const
{
    return HolderFactory_.ToIndexDict(this, list);
}

NUdf::TUnboxedValue TDefaultValueBuilder::NewArray(ui32 count, NUdf::TUnboxedValue*& itemsPtr) const {
    return HolderFactory_.CreateDirectArrayHolder(count, itemsPtr);
}

NUdf::TUnboxedValue TDefaultValueBuilder::NewVariant(ui32 index, NUdf::TUnboxedValue&& value) const {
    return HolderFactory_.CreateVariantHolder(value.Release(), index);
}

NUdf::IDictValueBuilder::TPtr TDefaultValueBuilder::NewDict(const NUdf::TType* dictType, ui32 flags) const
{
    return HolderFactory_.NewDict(dictType, flags);
}

bool TDefaultValueBuilder::MakeDate(ui32 year, ui32 month, ui32 day, ui16& value) const {
    return ::NKikimr::NMiniKQL::MakeDate(year, month, day, value);
}

bool TDefaultValueBuilder::SplitDate(ui16 value, ui32& year, ui32& month, ui32& day) const {
    return ::NKikimr::NMiniKQL::SplitDate(value, year, month, day);
}

bool TDefaultValueBuilder::MakeDatetime(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second, ui32& value, ui16 tzId) const
{
    return ::NKikimr::NMiniKQL::MakeTzDatetime(year, month, day, hour, minute, second, value, tzId);
}

bool TDefaultValueBuilder::SplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second, ui16 tzId) const
{
    return ::NKikimr::NMiniKQL::SplitTzDatetime(value, year, month, day, hour, minute, second, tzId);
}

bool TDefaultValueBuilder::FullSplitDate(ui16 value, ui32& year, ui32& month, ui32& day,
    ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek, ui16 tzId) const {
    ui32 unusedWeekOfYearIso8601 = 0;
    return ::NKikimr::NMiniKQL::SplitTzDate(value, year, month, day, dayOfYear, weekOfYear, unusedWeekOfYearIso8601, dayOfWeek, tzId);
}

bool TDefaultValueBuilder::FullSplitDate2(ui16 value, ui32& year, ui32& month, ui32& day,
    ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId) const {
    return ::NKikimr::NMiniKQL::SplitTzDate(value, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
}

bool TDefaultValueBuilder::FullSplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second,
    ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek, ui16 tzId) const {
    ui32 unusedWeekOfYearIso8601 = 0;
    return ::NKikimr::NMiniKQL::SplitTzDatetime(value, year, month, day, hour, minute, second, dayOfYear, weekOfYear, unusedWeekOfYearIso8601, dayOfWeek, tzId);
}

bool TDefaultValueBuilder::FullSplitDatetime2(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second,
    ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 tzId) const {
    return ::NKikimr::NMiniKQL::SplitTzDatetime(value, year, month, day, hour, minute, second, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, tzId);
}

bool TDefaultValueBuilder::EnrichDate(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek) const {
    ui32 unusedWeekOfYearIso8601 = 0;
    return ::NKikimr::NMiniKQL::EnrichDate(date, dayOfYear, weekOfYear, unusedWeekOfYearIso8601, dayOfWeek);
}

bool TDefaultValueBuilder::EnrichDate2(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const {
    return ::NKikimr::NMiniKQL::EnrichDate(date, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek);
}

bool TDefaultValueBuilder::GetTimezoneShift(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second, ui16 tzId, i32& value) const
{
    return ::NKikimr::NMiniKQL::GetTimezoneShift(year, month, day, hour, minute, second, tzId, value);
}

const NUdf::TSourcePosition* TDefaultValueBuilder::CalleePosition() const { 
    return *CalleePositionPtr_;
} 
 
NUdf::TUnboxedValue TDefaultValueBuilder::Run(const NUdf::TSourcePosition& callee, const NUdf::IBoxedValue& value, const NUdf::TUnboxedValuePod* args) const { 
    const auto prev = *CalleePositionPtr_;
    *CalleePositionPtr_ = &callee;
    const auto ret = NUdf::TBoxedValueAccessor::Run(value, this, args);
    *CalleePositionPtr_ = prev;
    return ret; 
} 
 
NUdf::TFlatDataBlockPtr TDefaultValueBuilder::NewFlatDataBlock(ui32 initialSize, ui32 initialCapacity) const { 
    return HolderFactory_.CreateFlatDataBlock(initialSize, initialCapacity); 
} 
 
NUdf::TFlatArrayBlockPtr TDefaultValueBuilder::NewFlatArrayBlock(ui32 count) const { 
    return HolderFactory_.CreateFlatArrayBlock(count); 
} 
 
NUdf::TSingleBlockPtr TDefaultValueBuilder::NewSingleBlock(const NUdf::TUnboxedValue& value) const { 
    return HolderFactory_.CreateSingleBlock(value); 
} 
 
bool TDefaultValueBuilder::FindTimezoneName(ui32 id, NUdf::TStringRef& name) const { 
    auto res = ::NKikimr::NMiniKQL::FindTimezoneIANAName(id); 
    if (!res) { 
        return false; 
    } 
 
    name = *res; 
    return true; 
} 
 
bool TDefaultValueBuilder::FindTimezoneId(const NUdf::TStringRef& name, ui32& id) const { 
    auto res = ::NKikimr::NMiniKQL::FindTimezoneId(name); 
    if (!res) { 
        return false; 
    } 
 
    id = *res; 
    return true; 
} 
 
bool TDefaultValueBuilder::GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef& value) const {
    if (SecureParamsProvider_)
        return SecureParamsProvider_->GetSecureParam(key, value);
    return false;
}
 
} // namespace NMiniKQL
} // namespace Nkikimr

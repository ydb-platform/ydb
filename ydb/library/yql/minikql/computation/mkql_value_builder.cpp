#include "mkql_value_builder.h"
#include "mkql_validate.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/utils.h>
#include <library/cpp/yson/node/node_io.h>

#include <arrow/chunked_array.h>
#include <arrow/array/array_base.h>
#include <arrow/array/util.h>
#include <arrow/c/bridge.h>

#include <util/system/env.h>

namespace NKikimr {
namespace NMiniKQL {

///////////////////////////////////////////////////////////////////////////////
// TDefaultValueBuilder
///////////////////////////////////////////////////////////////////////////////
TDefaultValueBuilder::TDefaultValueBuilder(const THolderFactory& holderFactory, NUdf::EValidatePolicy policy)
    : HolderFactory_(holderFactory)
    , Policy_(policy)
    , PgBuilder_(NYql::CreatePgBuilder())
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

    Cerr << fullMessage << Flush;
    _exit(1);
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
        return HolderFactory_.GetEmptyContainerLazy();

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

NUdf::TUnboxedValue TDefaultValueBuilder::NewArray32(ui32 count, NUdf::TUnboxedValue*& itemsPtr) const {
    return HolderFactory_.CreateDirectArrayHolder(count, itemsPtr);
}

NUdf::TUnboxedValue TDefaultValueBuilder::NewArray64(ui64 count, NUdf::TUnboxedValue*& itemsPtr) const {
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

void TDefaultValueBuilder::ExportArrowBlock(NUdf::TUnboxedValuePod value, ui32 chunk, ArrowArray* out) const {
    const auto& datum = TArrowBlock::From(value).GetDatum();
    std::shared_ptr<arrow::Array> arr;
    if (datum.is_scalar()) {
        if (chunk != 0) {
            UdfTerminate("Bad chunk index");
        }

        auto arrRes = arrow::MakeArrayFromScalar(*datum.scalar(), 1);
        if (!arrRes.status().ok()) {
            UdfTerminate(arrRes.status().ToString().c_str());
        }

        arr = std::move(arrRes).ValueOrDie();
    } else if (datum.is_array()) {
        if (chunk != 0) {
            UdfTerminate("Bad chunk index");
        }

        arr = datum.make_array();
    } else if (datum.is_arraylike()) {
        const auto& chunks = datum.chunks();
        if (chunk >= chunks.size()) {
            UdfTerminate("Bad chunk index");
        }

        arr = chunks[chunk];
    } else {
        UdfTerminate("Unexpected kind of arrow::Datum");
    }

    auto status = arrow::ExportArray(*arr, out);
    if (!status.ok()) {
        UdfTerminate(status.ToString().c_str());
    }
}

NUdf::TUnboxedValue TDefaultValueBuilder::ImportArrowBlock(ArrowArray* arrays, ui32 chunkCount, bool isScalar, const NUdf::IArrowType& type) const {
    const auto dataType = static_cast<const TArrowType&>(type).GetType();   
    if (isScalar) {
        if (chunkCount != 1) {
            UdfTerminate("Bad chunkCount value");
        }

        auto arrRes = arrow::ImportArray(arrays, dataType);
        auto arr = std::move(arrRes).ValueOrDie();
        if (arr->length() != 1) {
            UdfTerminate("Expected array with one element");
        }

        auto scalarRes = arr->GetScalar(0);
        if (!scalarRes.status().ok()) {
            UdfTerminate(scalarRes.status().ToString().c_str());
        }

        auto scalar = std::move(scalarRes).ValueOrDie();
        return HolderFactory_.CreateArrowBlock(std::move(scalar));
    } else {
        if (chunkCount < 1) {
            UdfTerminate("Bad chunkCount value");
        }

        TVector<std::shared_ptr<arrow::Array>> imported(chunkCount);
        for (ui32 i = 0; i < chunkCount; ++i) {
            auto arrRes = arrow::ImportArray(arrays + i, dataType);
            if (!arrRes.status().ok()) {
                UdfTerminate(arrRes.status().ToString().c_str());
            }

            imported[i] = std::move(arrRes).ValueOrDie();
        }

        if (chunkCount == 1) {
            return HolderFactory_.CreateArrowBlock(imported.front());
        } else {
            return HolderFactory_.CreateArrowBlock(arrow::ChunkedArray::Make(std::move(imported), dataType).ValueOrDie());
        }
    }
}

ui32 TDefaultValueBuilder::GetArrowBlockChunks(NUdf::TUnboxedValuePod value, bool& isScalar, ui64& length) const {
    const auto& datum = TArrowBlock::From(value).GetDatum();
    isScalar = false;
    length = datum.length();
    if (datum.is_scalar()) {
        isScalar = true;
        return 1;
    } else if (datum.is_array()) {
        return 1;
    } else if (datum.is_arraylike()) {
        return datum.chunks().size();
    } else {
        UdfTerminate("Unexpected kind of arrow::Datum");
    }
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

bool TDefaultValueBuilder::SplitTzDate32(i32 date, i32& year, ui32& month, ui32& day,
        ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId) const
{
    return ::NKikimr::NMiniKQL::SplitTzDate32(date, year, month, day, dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, timezoneId);
}

bool TDefaultValueBuilder::SplitTzDatetime64(i64 datetime, i32& year, ui32& month, ui32& day,
        ui32& hour, ui32& minute, ui32& second,
        ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId) const
{
    return ::NKikimr::NMiniKQL::SplitTzDatetime64(
                datetime, year, month, day, hour, minute, second,
                dayOfYear, weekOfYear, weekOfYearIso8601, dayOfWeek, timezoneId);
}

bool TDefaultValueBuilder::MakeTzDate32(i32 year, ui32 month, ui32 day, i32& date, ui16 timezoneId) const {
    return ::NKikimr::NMiniKQL::MakeTzDate32(year, month, day, date, timezoneId);
}

bool TDefaultValueBuilder::MakeTzDatetime64(i32 year, ui32 month, ui32 day,
        ui32 hour, ui32 minute, ui32 second, i64& datetime, ui16 timezoneId) const
{
    return ::NKikimr::NMiniKQL::MakeTzDatetime64(year, month, day, hour, minute, second, datetime, timezoneId);
}

} // namespace NMiniKQL
} // namespace Nkikimr

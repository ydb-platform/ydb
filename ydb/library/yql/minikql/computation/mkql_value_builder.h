#pragma once

#include "mkql_computation_node_holders.h"

#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_validate.h>

#include <util/generic/noncopyable.h>
#include <util/memory/pool.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>

namespace NKikimr {
namespace NMiniKQL {

///////////////////////////////////////////////////////////////////////////////
// TDefaultValueBuilder
///////////////////////////////////////////////////////////////////////////////
class TDefaultValueBuilder final: public NUdf::IValueBuilder, private TNonCopyable, public ITerminator,
    public NUdf::IDateBuilder
{
public:
    explicit TDefaultValueBuilder(const THolderFactory& holderFactory, NUdf::EValidatePolicy policy = NUdf::EValidatePolicy::Fail);

    void SetSecureParamsProvider(const NUdf::ISecureParamsProvider* provider);
    void RethrowAtTerminate();
    void SetCalleePositionHolder(const NUdf::TSourcePosition*& position);

    void Terminate(const char* message) const final;

    NUdf::TUnboxedValue NewStringNotFilled(ui32 size) const final;

    NUdf::TUnboxedValue ConcatStrings(NUdf::TUnboxedValuePod first, NUdf::TUnboxedValuePod second) const final;

    NUdf::TUnboxedValue AppendString(NUdf::TUnboxedValuePod value, const NUdf::TStringRef& ref) const final;
    NUdf::TUnboxedValue PrependString(const NUdf::TStringRef& ref,NUdf::TUnboxedValuePod value) const final;

    NUdf::TUnboxedValue SubString(NUdf::TUnboxedValuePod value, ui32 offset, ui32 size) const final;
    NUdf::TUnboxedValue NewList(NUdf::TUnboxedValue* items, ui64 count) const final;

    NUdf::TUnboxedValue NewString(const NUdf::TStringRef& ref) const final;

    NUdf::IDictValueBuilder::TPtr NewDict(const NUdf::TType* dictType, ui32 flags) const final;

    NUdf::TUnboxedValue ReverseList(const NUdf::TUnboxedValuePod& list) const final;
    NUdf::TUnboxedValue SkipList(const NUdf::TUnboxedValuePod& list, ui64 count) const final;
    NUdf::TUnboxedValue TakeList(const NUdf::TUnboxedValuePod& list, ui64 count) const final;
    NUdf::TUnboxedValue ToIndexDict(const NUdf::TUnboxedValuePod& list) const final;

    NUdf::TUnboxedValue NewArray32(ui32 count, NUdf::TUnboxedValue*& itemsPtr) const final;
    NUdf::TUnboxedValue NewVariant(ui32 index, NUdf::TUnboxedValue&& value) const final;
    const NUdf::IDateBuilder& GetDateBuilder() const final {
        return *this;
    }

    bool GetSecureParam(NUdf::TStringRef key, NUdf::TStringRef &value) const final;
    const NUdf::TSourcePosition* CalleePosition() const final;
    NUdf::TUnboxedValue Run(const NUdf::TSourcePosition& callee, const NUdf::IBoxedValue& value, const NUdf::TUnboxedValuePod* args) const final;
    void ExportArrowBlock(NUdf::TUnboxedValuePod value, ui32 chunk, ArrowArray* out) const final;
    NUdf::TUnboxedValue ImportArrowBlock(ArrowArray* arrays, ui32 chunkCount, bool isScalar, const NUdf::IArrowType& type) const final;
    ui32 GetArrowBlockChunks(NUdf::TUnboxedValuePod value, bool& isScalar, ui64& length) const final;
    bool MakeDate(ui32 year, ui32 month, ui32 day, ui16& value) const final;
    bool SplitDate(ui16 value, ui32& year, ui32& month, ui32& day) const final;

    bool MakeDatetime(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second, ui32& value, ui16 tzId = 0) const final;
    bool SplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second, ui16 tzId = 0) const final;

    bool EnrichDate(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek) const final;

    // in minutes
    bool GetTimezoneShift(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second, ui16 tzId, i32& value) const final;

    bool FullSplitDate(ui16 value, ui32& year, ui32& month, ui32& day,
        ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek, ui16 timezoneId = 0) const final;
    bool FullSplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second,
        ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek, ui16 timezoneId = 0) const final;

    bool FindTimezoneName(ui32 id, NUdf::TStringRef& name) const final;
    bool FindTimezoneId(const NUdf::TStringRef& name, ui32& id) const final;

    bool EnrichDate2(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const final;
    bool FullSplitDate2(ui16 value, ui32& year, ui32& month, ui32& day,
        ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const final;
    bool FullSplitDatetime2(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second,
        ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const final;

    const NUdf::IPgBuilder& GetPgBuilder() const final {
        return *PgBuilder_;
    }

    NUdf::TUnboxedValue NewArray64(ui64 count, NUdf::TUnboxedValue*& itemsPtr) const final;

    bool SplitTzDate32(i32 date, i32& year, ui32& month, ui32& day,
            ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const final;
    bool SplitTzDatetime64(i64 datetime, i32& year, ui32& month, ui32& day,
            ui32& hour, ui32& minute, ui32& second,
            ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const final;
    bool MakeTzDate32(i32 year, ui32 month, ui32 day, i32& date, ui16 timezoneId = 0) const final;
    bool MakeTzDatetime64(i32 year, ui32 month, ui32 day,
            ui32 hour, ui32 minute, ui32 second, i64& datetime, ui16 timezoneId = 0) const final;

private:
    const THolderFactory& HolderFactory_;
    NUdf::EValidatePolicy Policy_;
    std::unique_ptr<NUdf::IPgBuilder> PgBuilder_;
    const NUdf::ISecureParamsProvider* SecureParamsProvider_ = nullptr;
    const NUdf::TSourcePosition** CalleePositionPtr_ = nullptr;
    mutable bool Rethrow_ = false;
};

} // namespace NMiniKQL
} // namespace Nkikimr

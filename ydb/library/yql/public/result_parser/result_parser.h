#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/udf/udf_types.h>

namespace NYql {

struct IResultVisitor {
    virtual ~IResultVisitor() = default;

    // One row's set.
    virtual void OnWriteBegin() = 0;
    virtual void OnWriteEnd() = 0;

    // Header items of one row's set.
    virtual void OnLabel(const TStringBuf& label) = 0;
    virtual void OnPosition(const TPosition& pos) = 0;
    virtual void OnType(NUdf::ITypeVisitor*& typeVisitor) = 0;

    virtual void OnOptionalBegin() = 0;
    virtual void OnOptionalEnd() = 0;

    virtual void OnListBegin() = 0;
    virtual void OnListEnd() = 0;

    virtual void OnDictBegin() = 0;
    virtual void OnKeyBegin() = 0;
    virtual void OnKeyEnd() = 0;
    virtual void OnValueBegin() = 0;
    virtual void OnValueEnd() = 0;
    virtual void OnDictEnd() = 0;

    virtual void OnTupleBegin() = 0;
    virtual void OnElementBegin(ui64 index) = 0;
    virtual void OnElementBegin() = 0;
    virtual void OnTupleEnd() = 0;

    virtual void OnStructBegin() = 0;
    virtual void OnMemberBegin(const TStringBuf& name) = 0;
    virtual void OnMemberBegin() = 0;
    virtual void OnStructEnd() = 0;

    virtual void OnNull() = 0;
    virtual void OnBoolean(bool) = 0;

    virtual void OnUInt8(ui8) = 0;
    virtual void OnUInt16(ui16) = 0;
    virtual void OnUInt32(ui32) = 0;
    virtual void OnUInt64(ui64) = 0;

    virtual void OnInt8(i8) = 0;
    virtual void OnInt16(i16) = 0;
    virtual void OnInt32(i32) = 0;
    virtual void OnInt64(i64) = 0;

    virtual void OnFloat(float) = 0;
    virtual void OnDouble(double) = 0;

    virtual void OnBytes(const TStringBuf&) = 0;
    virtual void OnText(const TStringBuf&) = 0;

    // TODO: Other representations
};

struct TResultParseOptions {
    bool ParseTypesOnly = false;
};

void ParseResult(const TStringBuf& yson, IResultVisitor& visitor, const TResultParseOptions& options = {});

}

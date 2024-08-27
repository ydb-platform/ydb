#pragma once

#include "result_parser.h"

namespace NYql {

template<bool ThrowIfNotImplemented = true>
struct TResultVisitorBase : public IResultVisitor {

    void OnLabel(TStringBuf label) override;
    void OnPosition(const TPosition& pos)  override;

    void OnWriteBegin()  override;
    void OnWriteEnd() override;

    void OnType(NUdf::ITypeVisitor*& typeVisitor) override;

    void OnOptionalBegin() override;
    void OnOptionalEnd() override;

    void OnListBegin() override;
    void OnListEnd() override;


    void OnDictBegin() override;
    void OnKeyBegin() override;
    void OnKeyEnd() override;
    void OnValueBegin() override;
    void OnValueEnd() override;
    void OnDictEnd() override;

    void OnTupleBegin() override;
    void OnElementBegin() override;
    void OnElementEnd() override;
    void OnTupleEnd() override;

    void OnStructBegin() override;
    void OnMemberBegin(TStringBuf name) override;
    void OnMemberBegin() override;
    void OnStructEnd() override;

    void OnNull() override;
    void OnBoolean(bool) override;

    void OnUInt8(ui8) override;
    void OnUInt16(ui16) override;
    void OnUInt32(ui32) override;
    void OnUInt64(ui64) override;

    void OnInt8(i8) override;
    void OnInt16(i16) override;
    void OnInt32(i32) override;
    void OnInt64(i64) override;

    void OnFloat(float) override;
    void OnDouble(double) override;

    void OnBytes(TStringBuf) override;
    void OnText(TStringBuf) override;

};

}

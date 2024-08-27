#include "result_visitor_base.h"

namespace NYql {

#define THWROW_IF_NOT_IMPL \
    if constexpr (ThrowIfNotImplemented) { \
        throw yexception() << __func__ << " not implemented."; \
    }

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnLabel(const TStringBuf&) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnPosition(const TPosition&) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnWriteBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnWriteEnd() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnType(NUdf::ITypeVisitor*& typeVisitor) {
    typeVisitor = nullptr;
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnOptionalBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnOptionalEnd() {
        THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnListBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnListEnd() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnDictBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnKeyBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnKeyEnd() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnValueBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnValueEnd() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnDictEnd() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnTupleBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnElementBegin(ui64) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnElementBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnTupleEnd() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnStructBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnMemberBegin(const TStringBuf&) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnMemberBegin() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnStructEnd() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnNull() {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnBoolean(bool) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt8(ui8) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt16(ui16) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt32(ui32) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt64(ui64) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt8(i8) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt16(i16) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt32(i32) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt64(i64) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnFloat(float) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnDouble(double) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnBytes(const TStringBuf&) {
    THWROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnText(const TStringBuf&) {
    THWROW_IF_NOT_IMPL;
}

template struct TResultVisitorBase<true>;
template struct TResultVisitorBase<false>;

}

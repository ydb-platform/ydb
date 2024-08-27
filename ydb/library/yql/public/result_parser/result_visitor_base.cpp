#include "result_visitor_base.h"

namespace NYql {

#define THROW_IF_NOT_IMPL \
    if constexpr (ThrowIfNotImplemented) { \
        throw yexception() << __func__ << " not implemented."; \
    }

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnLabel(const TStringBuf&) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnPosition(const TPosition&) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnWriteBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnWriteEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnType(NUdf::ITypeVisitor*& typeVisitor) {
    typeVisitor = nullptr;
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnOptionalBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnOptionalEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnListBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnListEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnDictBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnKeyBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnKeyEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnValueBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnValueEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnDictEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnTupleBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnElementBegin(ui64) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnElementBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnTupleEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnStructBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnMemberBegin(const TStringBuf&) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnMemberBegin() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnStructEnd() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnNull() {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnBoolean(bool) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt8(ui8) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt16(ui16) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt32(ui32) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnUInt64(ui64) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt8(i8) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt16(i16) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt32(i32) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnInt64(i64) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnFloat(float) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnDouble(double) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnBytes(const TStringBuf&) {
    THROW_IF_NOT_IMPL;
}

template<bool ThrowIfNotImplemented>
void TResultVisitorBase<ThrowIfNotImplemented>::OnText(const TStringBuf&) {
    THROW_IF_NOT_IMPL;
}

template struct TResultVisitorBase<true>;
template struct TResultVisitorBase<false>;

}

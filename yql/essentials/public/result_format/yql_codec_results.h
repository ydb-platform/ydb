#pragma once

#include <library/cpp/yson/writer.h>
#include <util/generic/strbuf.h>
#include <util/string/cast.h>

namespace NYql::NResult {

// we should not write numbers as numbers ever
// write numbers as strings except Yson value where we use restricted dialect
// write bool as bool
// write null as entity
class TYsonResultWriter {
public:
    static constexpr TStringBuf VoidString = "Void";

public:
    explicit TYsonResultWriter(NYson::TYsonConsumerBase& writer)
        : Writer_(writer)
    {
    }
    void OnVoid() {
        Writer_.OnStringScalar(VoidString);
    }
    void OnNull() {
        Writer_.OnEntity();
    }
    void OnEmptyList() {
        Writer_.OnBeginList();
        Writer_.OnEndList();
    }
    void OnEmptyDict() {
        Writer_.OnBeginList();
        Writer_.OnEndList();
    }
    void OnEntity() {
        Writer_.OnEntity();
    }
    // numbers
    void OnInt64Scalar(i64 value) {
        WriteNumberAsString(value);
    }
    void OnUint64Scalar(ui64 value) {
        WriteNumberAsString(value);
    }
    void OnFloatScalar(float value) {
        Writer_.OnStringScalar(::FloatToString(value));
    }
    void OnDoubleScalar(double value) {
        Writer_.OnStringScalar(::FloatToString(value));
    }
    void OnBooleanScalar(bool value) {
        Writer_.OnBooleanScalar(value);
    }
    // strings
    void OnStringScalar(TStringBuf value);
    void OnUtf8StringScalar(TStringBuf value) {
        Writer_.OnStringScalar(value);
    }
    // list construction
    void OnBeginList() {
        Writer_.OnBeginList();
    }
    void OnListItem() {
        Writer_.OnListItem();
    }
    void OnEndList() {
        Writer_.OnEndList();
    }
    void OnBeginMap() {
        Writer_.OnBeginMap();
    }
    void OnKeyedItem(const TStringBuf& key) {
        Writer_.OnKeyedItem(key);
    }
    void OnEndMap() {
        Writer_.OnEndMap();
    }
    void OnBeginAttributes() {
        Writer_.OnBeginAttributes();
    }
    void OnEndAttributes() {
        Writer_.OnEndAttributes();
    }
    void OnRaw(TStringBuf ysonNode, NYT::NYson::EYsonType type) {
        Writer_.OnRaw(ysonNode, type);
    }

private:
    template <typename T>
    void WriteNumberAsString(T value) {
        Writer_.OnStringScalar(::ToString(value));
    }

private:
    NYson::TYsonConsumerBase& Writer_;
};
} // namespace NYql::NResult

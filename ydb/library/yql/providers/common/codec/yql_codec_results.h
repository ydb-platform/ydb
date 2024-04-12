#pragma once

#include <library/cpp/yson/writer.h>
#include <util/generic/strbuf.h>
#include <util/string/cast.h>

namespace NYql {
namespace NCommon {

// we should not write numbers as numbers ever
// write numbers as strings except Yson value where we use restricted dialect
// write bool as bool
// write null as entity
class TYsonResultWriter {
public:
    static constexpr TStringBuf VoidString = "Void";

public:
    explicit TYsonResultWriter(NYson::TYsonConsumerBase& writer)
        : Writer(writer)
    {
    }
    void OnVoid() {
        Writer.OnStringScalar(VoidString);
    }
    void OnNull() {
        Writer.OnEntity();
    }
    void OnEmptyList() {
        Writer.OnBeginList();
        Writer.OnEndList();
    }
    void OnEmptyDict() {
        Writer.OnBeginList();
        Writer.OnEndList();
    }
    void OnEntity() {
        Writer.OnEntity();
    }
    // numbers
    void OnInt64Scalar(i64 value) {
        WriteNumberAsString(value);
    }
    void OnUint64Scalar(ui64 value) {
        WriteNumberAsString(value);
    }
    void OnFloatScalar(float value) {
        Writer.OnStringScalar(::FloatToString(value));
    }
    void OnDoubleScalar(double value) {
        Writer.OnStringScalar(::FloatToString(value));
    }
    void OnBooleanScalar(bool value) {
        Writer.OnBooleanScalar(value);
    }
    // strings
    void OnStringScalar(TStringBuf value);
    void OnUtf8StringScalar(TStringBuf value) {
        Writer.OnStringScalar(value);
    }
    // list construction
    void OnBeginList() {
        Writer.OnBeginList();
    }
    void OnListItem() {
        Writer.OnListItem();
    }
    void OnEndList() {
        Writer.OnEndList();
    }
    void OnBeginMap() {
        Writer.OnBeginMap();
    }
    void OnKeyedItem(const TStringBuf& key) {
        Writer.OnKeyedItem(key);
    }
    void OnEndMap() {
        Writer.OnEndMap();
    }
    void OnBeginAttributes() {
        Writer.OnBeginAttributes();
    }
    void OnEndAttributes() {
        Writer.OnEndAttributes();
    }
    void OnRaw(TStringBuf ysonNode, NYT::NYson::EYsonType type) {
        Writer.OnRaw(ysonNode, type);
    }

private:
    template <typename T>
    void WriteNumberAsString(T value) {
        Writer.OnStringScalar(::ToString(value));
    }

private:
    NYson::TYsonConsumerBase& Writer;
};
}
}

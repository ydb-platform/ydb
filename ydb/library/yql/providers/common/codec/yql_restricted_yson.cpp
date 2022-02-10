#include "yql_restricted_yson.h"

#include <ydb/library/yql/utils/parse_double.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/parser.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>

#include <util/generic/algorithm.h>
#include <util/generic/stack.h>

namespace NYql {
namespace NCommon {

namespace {
class TRestrictedYsonFormatter : public NYson::TYsonConsumerBase {
public:
    TRestrictedYsonFormatter(TYsonResultWriter& writer)
        : Writer(writer) {
    }

    void OnStringScalar(TStringBuf value) override {
        Open();
        Type(TStringBuf("string"));

        Buffer.clear();
        bool isAscii = true;
        for (size_t i = 0; i < value.size(); ++i) {
            if (ui8(value[i]) < 128) {
                if (!isAscii) {
                    Buffer.push_back(value[i]);
                }
            } else {
                if (isAscii) {
                    Buffer.resize(i);
                    Copy(value.data(), value.data() + i, Buffer.data());
                    isAscii = false;
                }
                Buffer.push_back('\xC0' | (ui8(value[i]) >> 6));
                Buffer.push_back('\x80' | (ui8(value[i]) & ~'\xC0'));
            }
        }

        if (isAscii) {
            Value(value);
        } else {
            Value(TStringBuf(Buffer.data(), Buffer.size()));
        }

        Close();
    }

    void OnInt64Scalar(i64 value) override {
        Open();
        Type(TStringBuf("int64"));
        Value(ToString(value));
        Close();
    }

    void OnUint64Scalar(ui64 value) override {
        Open();
        Type(TStringBuf("uint64"));
        Value(ToString(value));
        Close();
    }

    void OnDoubleScalar(double value) override {
        Open();
        Type(TStringBuf("double"));
        Value(::FloatToString(value));
        Close();
    }

    void OnBooleanScalar(bool value) override {
        Open();
        Type(TStringBuf("boolean"));
        Value(value ? TStringBuf("true") : TStringBuf("false"));
        Close();
    }

    void OnEntity() override {
        if (AfterAttributes) {
            Writer.OnKeyedItem(TStringBuf("$value"));
            Writer.OnEntity();
            Writer.OnEndMap();
            AfterAttributes = false;
        } else {
            Writer.OnEntity();
        }
    }

    void OnBeginList() override {
        if (AfterAttributes) {
            Writer.OnKeyedItem(TStringBuf("$value"));
        }

        Writer.OnBeginList();
        HasAttributes.push(AfterAttributes);
        AfterAttributes = false;
    }

    void OnListItem() override {
        Writer.OnListItem();
    }

    void OnEndList() override {
        Writer.OnEndList();
        if (HasAttributes.top()) {
            Writer.OnEndMap();
        }

        HasAttributes.pop();
    }

    void OnBeginMap() override {
        if (AfterAttributes) {
            Writer.OnKeyedItem(TStringBuf("$value"));
        }

        Writer.OnBeginMap();
        HasAttributes.push(AfterAttributes);
        AfterAttributes = false;
    }

    void OnKeyedItem(TStringBuf key) override {
        if (key.StartsWith('$')) {
            Writer.OnKeyedItem(TString("$") + key);
        } else {
            Writer.OnKeyedItem(key);
        }
    }

    void OnEndMap() override {
        Writer.OnEndMap();
        if (HasAttributes.top()) {
            Writer.OnEndMap();
        }

        HasAttributes.pop();
    }

    void OnBeginAttributes() override {
        Writer.OnBeginMap();
        Writer.OnKeyedItem(TStringBuf("$attributes"));
        Writer.OnBeginMap();
    }

    void OnEndAttributes() override {
        Writer.OnEndMap();
        AfterAttributes = true;
    }

    void Open() {
        if (!AfterAttributes) {
            Writer.OnBeginMap();
        }
    }

    void Close() {
        Writer.OnEndMap();
        AfterAttributes = false;
    }

    void Type(const TStringBuf& type) {
        Writer.OnKeyedItem(TStringBuf("$type"));
        Writer.OnUtf8StringScalar(type);
    }

    void Value(const TStringBuf& value) {
        Writer.OnKeyedItem(TStringBuf("$value"));
        Writer.OnUtf8StringScalar(value);
    }

private:
    TYsonResultWriter& Writer;
    TStack<bool> HasAttributes;
    bool AfterAttributes = false;
    TVector<char> Buffer;
};

TString DecodeRestrictedBinaryString(const TString& data) {
    TString res;
    for (size_t i = 0; i < data.size(); ++i) {
        char c = data[i];
        if (((unsigned char)c) >= 128) {
            YQL_ENSURE(i + 1 < data.size());
            res.push_back(((c & 0x03) << 6) | (data[i + 1] & 0x3f));
            ++i;
        } else {
            res.push_back(c);
        }
    }

    return res;
}

void DecodeRestrictedYson(const NYT::TNode& node, NYson::TYsonConsumerBase& writer) {
    switch (node.GetType()) {
    case NYT::TNode::String:
        writer.OnStringScalar(node.AsString());
        return;

    case NYT::TNode::Int64:
        writer.OnInt64Scalar(node.AsInt64());
        return;

    case NYT::TNode::Uint64:
        writer.OnUint64Scalar(node.AsUint64());
        return;

    case NYT::TNode::Bool:
        writer.OnBooleanScalar(node.AsBool());
        return;

    case NYT::TNode::Double:
        writer.OnDoubleScalar(node.AsDouble());
        return;

    case NYT::TNode::Null:
        writer.OnEntity();
        return;

    case NYT::TNode::List:
        // just a list without attributes
        writer.OnBeginList();
        for (const auto& item : node.AsList()) {
            writer.OnListItem();
            DecodeRestrictedYson(item, writer);
        }

        writer.OnEndList();
        return;

    case NYT::TNode::Map:
        // process below
        break;

    default:
        YQL_ENSURE(false, "Unsupported node type: " << static_cast<int>(node.GetType()));
    }

    YQL_ENSURE(node.IsMap());
    if (!node.HasKey("$value")) {
        // just a map without attributes
        writer.OnBeginMap();
        for (const auto& x : node.AsMap()) {
            if (x.first.StartsWith("$$")) {
                writer.OnKeyedItem(x.first.substr(1));
            } else {
                writer.OnKeyedItem(x.first);
            }

            DecodeRestrictedYson(x.second, writer);
        }

        writer.OnEndMap();
        return;
    }

    if (node.HasKey("$attributes")) {
        writer.OnBeginAttributes();
        for (const auto& x : node["$attributes"].AsMap()) {
            if (x.first.StartsWith("$$")) {
                writer.OnKeyedItem(x.first.substr(1));
            } else {
                writer.OnKeyedItem(x.first);
            }

            DecodeRestrictedYson(x.second, writer);
        }

        writer.OnEndAttributes();
    }

    if (!node.HasKey("$type")) {
        // non-scalars with attributes
        DecodeRestrictedYson(node["$value"], writer);
        return;
    }

    auto type = node["$type"].AsString();
    if (type == "int64") {
        writer.OnInt64Scalar(FromString<i64>(node["$value"].AsString()));
    } else if (type == "uint64") {
        writer.OnUint64Scalar(FromString<ui64>(node["$value"].AsString()));
    } else if (type == "double") {
        writer.OnDoubleScalar(DoubleFromString(TStringBuf(node["$value"].AsString())));
    } else if (type == "boolean") {
        writer.OnBooleanScalar(FromString<bool>(node["$value"].AsString()));
    } else if (type == "string") {
        writer.OnStringScalar(DecodeRestrictedBinaryString(node["$value"].AsString()));
    } else {
        YQL_ENSURE(false, "Unsupported type: " << type);
    }
}

} // anonymous namespace

void EncodeRestrictedYson(TYsonResultWriter& writer, const TStringBuf& yson) {
    TRestrictedYsonFormatter formatter(writer);
    NYson::ParseYsonStringBuffer(yson, &formatter);
}

TString EncodeRestrictedYson(const NYT::TNode& node, NYson::EYsonFormat format) {
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, format);
    TYsonResultWriter resultWriter(writer);
    TRestrictedYsonFormatter formatter(resultWriter);
    NYT::TNodeVisitor visitor(&formatter);
    visitor.Visit(node);
    return stream.Str();
}

TString DecodeRestrictedYson(const TStringBuf& yson, NYson::EYsonFormat format) {
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, format);
    auto node = NYT::NodeFromYsonString(yson);
    DecodeRestrictedYson(node, writer);
    return stream.Str();
}

}
}

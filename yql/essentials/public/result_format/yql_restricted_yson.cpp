#include "yql_restricted_yson.h"

#include <yql/essentials/utils/parse_double.h>
#include <yql/essentials/utils/yql_panic.h>

#include <library/cpp/yson/detail.h>
#include <library/cpp/yson/parser.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/node/node_visitor.h>

#include <util/generic/algorithm.h>
#include <util/generic/stack.h>

namespace NYql::NResult {

namespace {
class TRestrictedYsonFormatter: public NYson::TYsonConsumerBase {
public:
    explicit TRestrictedYsonFormatter(TYsonResultWriter& writer)
        : Writer_(writer)
    {
    }

    void OnStringScalar(TStringBuf value) override {
        Open();
        Type(TStringBuf("string"));

        Buffer_.clear();
        bool isAscii = true;
        for (size_t i = 0; i < value.size(); ++i) {
            if (ui8(value[i]) < 128) {
                if (!isAscii) {
                    Buffer_.push_back(value[i]);
                }
            } else {
                if (isAscii) {
                    Buffer_.resize(i);
                    Copy(value.data(), value.data() + i, Buffer_.data());
                    isAscii = false;
                }
                Buffer_.push_back('\xC0' | (ui8(value[i]) >> 6));
                Buffer_.push_back('\x80' | (ui8(value[i]) & ~'\xC0'));
            }
        }

        if (isAscii) {
            Value(value);
        } else {
            Value(TStringBuf(Buffer_.data(), Buffer_.size()));
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
        if (AfterAttributes_) {
            Writer_.OnKeyedItem(TStringBuf("$value"));
            Writer_.OnEntity();
            Writer_.OnEndMap();
            AfterAttributes_ = false;
        } else {
            Writer_.OnEntity();
        }
    }

    void OnBeginList() override {
        if (AfterAttributes_) {
            Writer_.OnKeyedItem(TStringBuf("$value"));
        }

        Writer_.OnBeginList();
        HasAttributes_.push(AfterAttributes_);
        AfterAttributes_ = false;
    }

    void OnListItem() override {
        Writer_.OnListItem();
    }

    void OnEndList() override {
        Writer_.OnEndList();
        if (HasAttributes_.top()) {
            Writer_.OnEndMap();
        }

        HasAttributes_.pop();
    }

    void OnBeginMap() override {
        if (AfterAttributes_) {
            Writer_.OnKeyedItem(TStringBuf("$value"));
        }

        Writer_.OnBeginMap();
        HasAttributes_.push(AfterAttributes_);
        AfterAttributes_ = false;
    }

    void OnKeyedItem(TStringBuf key) override {
        if (key.StartsWith('$')) {
            Writer_.OnKeyedItem(TString("$") + key);
        } else {
            Writer_.OnKeyedItem(key);
        }
    }

    void OnEndMap() override {
        Writer_.OnEndMap();
        if (HasAttributes_.top()) {
            Writer_.OnEndMap();
        }

        HasAttributes_.pop();
    }

    void OnBeginAttributes() override {
        Writer_.OnBeginMap();
        Writer_.OnKeyedItem(TStringBuf("$attributes"));
        Writer_.OnBeginMap();
    }

    void OnEndAttributes() override {
        Writer_.OnEndMap();
        AfterAttributes_ = true;
    }

    void Open() {
        if (!AfterAttributes_) {
            Writer_.OnBeginMap();
        }
    }

    void Close() {
        Writer_.OnEndMap();
        AfterAttributes_ = false;
    }

    void Type(const TStringBuf& type) {
        Writer_.OnKeyedItem(TStringBuf("$type"));
        Writer_.OnUtf8StringScalar(type);
    }

    void Value(const TStringBuf& value) {
        Writer_.OnKeyedItem(TStringBuf("$value"));
        Writer_.OnUtf8StringScalar(value);
    }

private:
    TYsonResultWriter& Writer_;
    TStack<bool> HasAttributes_;
    bool AfterAttributes_ = false;
    TVector<char> Buffer_;
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

TString DecodeRestrictedYson(const NYT::TNode& node, NYson::EYsonFormat format) {
    TStringStream stream;
    NYson::TYsonWriter writer(&stream, format);
    DecodeRestrictedYson(node, writer);
    return stream.Str();
}

TString DecodeRestrictedYson(const TStringBuf& yson, NYson::EYsonFormat format) {
    return DecodeRestrictedYson(NYT::NodeFromYsonString(yson), format);
}

} // namespace NYql::NResult

#include "yson_helpers.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/utils/log/context.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/yson/detail.h>

#include <util/generic/yexception.h>

namespace NYql {

TBinaryYsonWriter::TBinaryYsonWriter(IOutputStream* stream, NYson::EYsonType type)
    : NYson::TYsonWriter(stream, NYson::EYsonFormat::Binary, type, false)
{
    if (type != ::NYson::EYsonType::Node) {
        Stack_.push_back(false);
    }
}

void TBinaryYsonWriter::OnBeginList() {
    Stack_.push_back(false);
    NYson::TYsonWriter::OnBeginList();
}

void TBinaryYsonWriter::OnListItem() {
    Stack_.back() = true;
    NYson::TYsonWriter::OnListItem();
}

void TBinaryYsonWriter::OnEndList() {
    if (Stack_.back()) {
        Stream->Write(NYson::NDetail::ListItemSeparatorSymbol);
    }
    Stack_.pop_back();
    NYson::TYsonWriter::OnEndList();
}

void TBinaryYsonWriter::OnBeginMap() {
    Stack_.push_back(false);
    NYson::TYsonWriter::OnBeginMap();
}

void TBinaryYsonWriter::OnKeyedItem(TStringBuf key) {
    Stack_.back() = true;
    NYson::TYsonWriter::OnKeyedItem(key);
}

void TBinaryYsonWriter::OnEndMap() {
    if (Stack_.back()) {
        Stream->Write(NYson::NDetail::KeyedItemSeparatorSymbol);
    }
    Stack_.pop_back();
    NYson::TYsonWriter::OnEndMap();
}

void TBinaryYsonWriter::OnBeginAttributes() {
    Stack_.push_back(false);
    NYson::TYsonWriter::OnBeginAttributes();
}

void TBinaryYsonWriter::OnEndAttributes() {
    if (Stack_.back()) {
        Stream->Write(NYson::NDetail::KeyedItemSeparatorSymbol);
    }
    Stack_.pop_back();
    NYson::TYsonWriter::OnEndAttributes();
}


TColumnFilteringConsumer::TColumnFilteringConsumer(NYT::NYson::IYsonConsumer* parent, const TMaybe<TSet<TStringBuf>>& columns,
                                                   const TMaybe<THashMap<TStringBuf, TStringBuf>>& renameColumns)
    : Parent_(parent)
    , Columns_(columns)
    , RenameColumns_(renameColumns)
{
}

void TColumnFilteringConsumer::OnStringScalar(TStringBuf value) {
    if (Enable_) {
        Parent_->OnStringScalar(value);
    }
}

void TColumnFilteringConsumer::OnInt64Scalar(i64 value) {
    if (Enable_) {
        Parent_->OnInt64Scalar(value);
    }
}

void TColumnFilteringConsumer::OnUint64Scalar(ui64 value) {
    if (Enable_) {
        Parent_->OnUint64Scalar(value);
    }
}

void TColumnFilteringConsumer::OnDoubleScalar(double value) {
    if (Enable_) {
        Parent_->OnDoubleScalar(value);
    }
}

void TColumnFilteringConsumer::OnBooleanScalar(bool value) {
    if (Enable_) {
        Parent_->OnBooleanScalar(value);
    }
}

void TColumnFilteringConsumer::OnEntity() {
    if (Enable_) {
        Parent_->OnEntity();
    }
}

void TColumnFilteringConsumer::OnBeginList() {
    Stack_.push_back(Enable_);
    if (Enable_) {
        Parent_->OnBeginList();
    }
}

void TColumnFilteringConsumer::OnListItem() {
    if (Enable_) {
        Parent_->OnListItem();
    }
}

void TColumnFilteringConsumer::OnEndList() {
    Enable_ = Stack_.back();
    Stack_.pop_back();
    if (Enable_) {
        Parent_->OnEndList();
    }
}

void TColumnFilteringConsumer::OnBeginMap() {
    Stack_.push_back(Enable_);
    if (Enable_) {
        Parent_->OnBeginMap();
    }
}

void TColumnFilteringConsumer::OnKeyedItem(TStringBuf key) {
    TStringBuf outputKey = key;
    if (Stack_.size() == 1) {
        if (RenameColumns_) {
            auto r = RenameColumns_->find(key);
            if (r != RenameColumns_->end()) {
                outputKey = r->second;
            }
        }
        Enable_ = !Columns_ || Columns_->find(outputKey) != Columns_->end();
    }
    if (Enable_) {
        Parent_->OnKeyedItem(outputKey);
    }
}

void TColumnFilteringConsumer::OnEndMap() {
    Enable_ = Stack_.back();
    Stack_.pop_back();
    if (Enable_) {
        Parent_->OnEndMap();
    }
}

void TColumnFilteringConsumer::OnBeginAttributes() {
    if (Stack_.back()) {
        Parent_->OnBeginAttributes();
    }
}

void TColumnFilteringConsumer::OnEndAttributes() {
    if (Enable_) {
        Parent_->OnEndAttributes();
    }
}

void TColumnFilteringConsumer::OnRaw(TStringBuf yson, NYson::EYsonType type) {
    if (Enable_) {
        Parent_->OnRaw(yson, type);
    }
}

TDoubleHighPrecisionYsonWriter::TDoubleHighPrecisionYsonWriter(IOutputStream* stream, NYson::EYsonType type, bool enableRaw)
    : TYsonWriter(stream, NYson::EYsonFormat::Text, type, enableRaw)
{
}

void TDoubleHighPrecisionYsonWriter::OnDoubleScalar(double value) {
    TString str;
    if (std::isfinite(value)) {
        str = ::FloatToString(value);
    } else {
        if (std::isnan(value)) {
            str = TStringBuf("%nan");
        } else if (value > 0) {
            str = TStringBuf("%inf");
        } else {
            str = TStringBuf("%-inf");
        }
    }

    Stream->Write(str);
    if (str.find('.') == TString::npos && str.find('e') == TString::npos && std::isfinite(value)) {
        Stream->Write(".");
    }
    EndNode();
}

void WriteTableReference(NYson::TYsonWriter& writer, TStringBuf provider, TStringBuf cluster, TStringBuf table,
    bool remove, const TVector<TString>& columns)
{
    writer.OnBeginMap();
    writer.OnKeyedItem("Reference");
    writer.OnBeginList();
    writer.OnListItem();
    writer.OnStringScalar(provider);
    writer.OnListItem();
    writer.OnStringScalar(cluster);
    writer.OnListItem();
    writer.OnStringScalar(table);
    writer.OnEndList();
    if (!columns.empty()) {
        writer.OnKeyedItem("Columns");
        writer.OnBeginList();
        for (auto& column: columns) {
            writer.OnListItem();
            writer.OnStringScalar(column);
        }
        writer.OnEndList();
    }
    writer.OnKeyedItem("Remove");
    writer.OnBooleanScalar(remove);
    writer.OnEndMap();
}

bool HasYqlRowSpec(const NYT::TNode& inAttrs)
{
    bool first = true;
    bool hasScheme = false;
    for (auto& attrs: inAttrs.AsList()) {
        bool tableHasScheme = attrs.HasKey(YqlRowSpecAttribute);
        if (first) {
            hasScheme = tableHasScheme;
            first = false;
        } else {
            YQL_ENSURE(hasScheme == tableHasScheme, "Different schemas of input tables");
        }
    }
    return hasScheme;
}

bool HasYqlRowSpec(const TString& inputAttrs)
{
    NYT::TNode inAttrs;
    try {
        inAttrs = NYT::NodeFromYsonString(inputAttrs);
    } catch (const yexception& e) {
        YQL_LOG_CTX_THROW yexception() << "Invalid input meta attrs: " << e.what();
    }
    YQL_ENSURE(inAttrs.IsList(), "Expect List type of input meta attrs, but got type " << inAttrs.GetType());
    return HasYqlRowSpec(inAttrs);
}

bool HasStrictSchema(const NYT::TNode& attrs)
{
    if (attrs.HasKey(YqlRowSpecAttribute) && attrs[YqlRowSpecAttribute].HasKey(RowSpecAttrStrictSchema)) {
        auto& strictSchemaAttr = attrs[YqlRowSpecAttribute][RowSpecAttrStrictSchema];
        return strictSchemaAttr.IsInt64()
            ? strictSchemaAttr.AsInt64() != 0
            : NYT::GetBool(strictSchemaAttr);
    }
    return true;
}

TMaybe<ui64> GetDataWeight(const NYT::TNode& tableAttrs) {
    ui64 ret = 0;
    bool hasAttr = false;
    if (tableAttrs.AsMap().contains("data_weight")) {
        ret = (ui64)tableAttrs["data_weight"].AsInt64();
        hasAttr = true;
    }

    if (!ret && tableAttrs.AsMap().contains("uncompressed_data_size")) {
        ret = (ui64)tableAttrs["uncompressed_data_size"].AsInt64();
        hasAttr = true;
    }

    if (hasAttr) {
        return ret;
    }

    return Nothing();
}

ui64 GetTableRowCount(const NYT::TNode& tableAttrs) {
    return NYT::GetBool(tableAttrs[TStringBuf("dynamic")])
        ? tableAttrs[TStringBuf("chunk_row_count")].AsInt64()
        : tableAttrs[TStringBuf("row_count")].AsInt64();
}

ui64 GetContentRevision(const NYT::TNode& tableAttrs) {
    return tableAttrs.HasKey("content_revision")
        ? tableAttrs["content_revision"].IntCast<ui64>()
        : tableAttrs["revision"].IntCast<ui64>();
}

} // NYql

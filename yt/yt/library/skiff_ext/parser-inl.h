#ifndef PARSER_INL_H_
#error "Direct inclusion of this file is not allowed, include parser.h"
// For the sake of sane code completion.
#include "parser.h"
#endif

#include <library/cpp/skiff/skiff.h>

#include <yt/yt/core/concurrency/coroutine.h>

namespace NYT::NSkiffExt {

using namespace NSkiff;

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
class TSkiffMultiTableParser<TConsumer>::TImpl
{
public:
    TImpl(
        TConsumer* consumer,
        TSkiffSchemaList skiffSchemaList,
        const std::vector<TSkiffTableColumnIds>& tablesColumnIds,
        const TString& rangeIndexColumnName,
        const TString& rowIndexColumnName)
        : Consumer_(consumer)
        , SkiffSchemaList_(std::move(skiffSchemaList))
    {
        auto genericTableDescriptions = CreateTableDescriptionList(SkiffSchemaList_, rangeIndexColumnName, rowIndexColumnName);
        YT_VERIFY(tablesColumnIds.size() == genericTableDescriptions.size());

        for (size_t tableIndex = 0; tableIndex < genericTableDescriptions.size(); ++tableIndex) {
            YT_VERIFY(tablesColumnIds[tableIndex].DenseFieldColumnIds.size() == genericTableDescriptions[tableIndex].DenseFieldDescriptionList.size());
            const auto& genericTableDescription = genericTableDescriptions[tableIndex];
            auto& parserTableDescription = TableDescriptions_.emplace_back();
            parserTableDescription.HasOtherColumns = genericTableDescription.HasOtherColumns;
            for (size_t fieldIndex = 0; fieldIndex < genericTableDescription.DenseFieldDescriptionList.size(); ++fieldIndex) {
                const auto& denseFieldDescription = genericTableDescription.DenseFieldDescriptionList[fieldIndex];
                parserTableDescription.DenseFields.emplace_back(
                    denseFieldDescription.Name(),
                    denseFieldDescription.ValidatedSimplify(),
                    tablesColumnIds[tableIndex].DenseFieldColumnIds[fieldIndex],
                    denseFieldDescription.IsRequired());
            }

            YT_VERIFY(tablesColumnIds[tableIndex].SparseFieldColumnIds.size() == genericTableDescriptions[tableIndex].SparseFieldDescriptionList.size());

            for (size_t fieldIndex = 0;
                 fieldIndex < tablesColumnIds[tableIndex].SparseFieldColumnIds.size();
                 ++fieldIndex)
            {
                const auto& fieldDescription = genericTableDescriptions[tableIndex].SparseFieldDescriptionList[fieldIndex];
                parserTableDescription.SparseFields.emplace_back(
                    fieldDescription.Name(),
                    fieldDescription.ValidatedSimplify(),
                    tablesColumnIds[tableIndex].SparseFieldColumnIds[fieldIndex],
                    true);
            }
        }
    }

    Y_FORCE_INLINE void ParseField(ui16 columnId, const TString& name, EWireType wireType, bool required = false)
    {
        if (!required) {
            ui8 tag = Parser_->ParseVariant8Tag();
            if (tag == 0) {
                Consumer_->OnEntity(columnId);
                return;
            } else if (tag > 1) {
                THROW_ERROR_EXCEPTION(
                    "Found bad variant8 tag %Qv when parsing optional field %Qv",
                    tag,
                    name);
            }
        }
        switch (wireType) {
            case EWireType::Yson32:
                Consumer_->OnYsonString(Parser_->ParseYson32(), columnId);
                break;
            case EWireType::Int64:
                Consumer_->OnInt64Scalar(Parser_->ParseInt64(), columnId);
                break;
            case EWireType::Uint64:
                Consumer_->OnUint64Scalar(Parser_->ParseUint64(), columnId);
                break;
            case EWireType::Double:
                Consumer_->OnDoubleScalar(Parser_->ParseDouble(), columnId);
                break;
            case EWireType::Boolean:
                Consumer_->OnBooleanScalar(Parser_->ParseBoolean(), columnId);
                break;
            case EWireType::String32:
                Consumer_->OnStringScalar(Parser_->ParseString32(), columnId);
                break;
            default:
                // Other types should be filtered out when we parse skiff schema.
                YT_ABORT();
        }
    }

    void DoParse(IZeroCopyInput* stream)
    {
        Parser_ = std::make_unique<TCheckedInDebugSkiffParser>(CreateVariant16Schema(SkiffSchemaList_), stream);

        while (Parser_->HasMoreData()) {
            auto tag = Parser_->ParseVariant16Tag();
            if (tag >= TableDescriptions_.size()) {
                THROW_ERROR_EXCEPTION("Unknown table index varint16 tag")
                    << TErrorAttribute("tag", tag);
            }

            Consumer_->OnBeginRow(tag);

            for (const auto& field : TableDescriptions_[tag].DenseFields) {
                ParseField(field.ColumnId, field.Name, field.WireType, field.Required);
            }

            if (!TableDescriptions_[tag].SparseFields.empty()) {
                for (auto sparseFieldIdx = Parser_->ParseVariant16Tag();
                    sparseFieldIdx != EndOfSequenceTag<ui16>();
                    sparseFieldIdx = Parser_->ParseVariant16Tag())
                {
                    if (sparseFieldIdx >= TableDescriptions_[tag].SparseFields.size()) {
                        THROW_ERROR_EXCEPTION("Bad sparse field index %Qv, total sparse field count %Qv",
                            sparseFieldIdx,
                            TableDescriptions_[tag].SparseFields.size());
                    }

                    const auto& field = TableDescriptions_[tag].SparseFields[sparseFieldIdx];
                    ParseField(field.ColumnId, field.Name, field.WireType, true);
                }
            }

            if (TableDescriptions_[tag].HasOtherColumns) {
                auto buf = Parser_->ParseYson32();
                Consumer_->OnOtherColumns(buf);
            }

            Consumer_->OnEndRow();
        }
    }

    ui64 GetReadBytesCount()
    {
        return Parser_->GetReadBytesCount();
    }

private:
    struct TField
    {
        TString Name;
        EWireType WireType;
        ui16 ColumnId = 0;
        bool Required = false;

        TField(TString name, EWireType wireType, ui16 columnId, bool required)
            : Name(std::move(name))
            , WireType(wireType)
            , ColumnId(columnId)
            , Required(required)
        { }
    };

    struct TTableDescription
    {
        std::vector<TField> DenseFields;
        std::vector<TField> SparseFields;
        bool HasOtherColumns = false;
    };

    TConsumer* const Consumer_;
    TSkiffSchemaList SkiffSchemaList_;

    std::unique_ptr<TCheckedInDebugSkiffParser> Parser_;
    std::vector<TTableDescription> TableDescriptions_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TConsumer>
TSkiffMultiTableParser<TConsumer>::TSkiffMultiTableParser(
    TConsumer* consumer,
    TSkiffSchemaList schemaList,
    const std::vector<TSkiffTableColumnIds>& tablesColumnIds,
    const TString& rangeIndexColumnName,
    const TString& rowIndexColumnName)
    : ParserImpl_(new TImpl(consumer,
        schemaList,
        tablesColumnIds,
        rangeIndexColumnName,
        rowIndexColumnName))
    , ParserCoroPipe_(BIND([this] (IZeroCopyInput* stream) {
        ParserImpl_->DoParse(stream);
    }))
{ }

template <class TConsumer>
TSkiffMultiTableParser<TConsumer>::~TSkiffMultiTableParser()
{ }

template <class TConsumer>
void TSkiffMultiTableParser<TConsumer>::Read(TStringBuf data)
{
    ParserCoroPipe_.Feed(data);
}

template <class TConsumer>
void TSkiffMultiTableParser<TConsumer>::Finish()
{
    ParserCoroPipe_.Finish();
}

template <class TConsumer>
ui64 TSkiffMultiTableParser<TConsumer>::GetReadBytesCount()
{
    return ParserImpl_->GetReadBytesCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiffExt

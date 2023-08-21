#include "wire_row_stream.h"
#include "row_stream.h"
#include "helpers.h"

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/wire_protocol.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/core/misc/range.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NApi::NRpcProxy {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TWireRowStreamEncoder
    : public IRowStreamEncoder
{
public:
    explicit TWireRowStreamEncoder(TNameTablePtr nameTable)
        : NameTable_(std::move(nameTable))
    { }

    TSharedRef Encode(
        const IUnversionedRowBatchPtr& batch,
        const NApi::NRpcProxy::NProto::TRowsetStatistics* statistics) override
    {
        YT_VERIFY(NameTableSize_ <= NameTable_->GetSize());

        NProto::TRowsetDescriptor descriptor;
        descriptor.set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
        descriptor.set_rowset_kind(NApi::NRpcProxy::NProto::RK_UNVERSIONED);
        descriptor.set_rowset_format(NApi::NRpcProxy::NProto::RF_YT_WIRE);
        for (int id = NameTableSize_; id < NameTable_->GetSize(); ++id) {
            auto* entry = descriptor.add_name_table_entries();
            entry->set_name(TString(NameTable_->GetName(id)));
        }
        NameTableSize_ += descriptor.name_table_entries_size();

        auto writer = CreateWireProtocolWriter();
        auto rows = batch->MaterializeRows();
        writer->WriteUnversionedRowset(rows);
        auto rowRefs = writer->Finish();

        auto [block, payloadRef] = SerializeRowStreamBlockEnvelope(
            GetByteSize(rowRefs),
            descriptor,
            statistics);

        MergeRefsToRef(rowRefs, payloadRef);

        return block;
    }

private:
    const TNameTablePtr NameTable_;

    int NameTableSize_ = 0;
};

IRowStreamEncoderPtr CreateWireRowStreamEncoder(TNameTablePtr nameTable)
{
    return New<TWireRowStreamEncoder>(std::move(nameTable));
}

////////////////////////////////////////////////////////////////////////////////

class TWireRowStreamDecoder
    : public IRowStreamDecoder
{
public:
    explicit TWireRowStreamDecoder(TNameTablePtr nameTable)
        : NameTable_(std::move(nameTable))
    {
        Descriptor_.set_wire_format_version(NApi::NRpcProxy::CurrentWireFormatVersion);
        Descriptor_.set_rowset_kind(NApi::NRpcProxy::NProto::RK_UNVERSIONED);
    }

    IUnversionedRowBatchPtr Decode(
        const TSharedRef& payloadRef,
        const NProto::TRowsetDescriptor& descriptorDelta) override
    {
        struct TWireRowStreamDecoderTag { };
        auto reader = CreateWireProtocolReader(payloadRef, New<TRowBuffer>(TWireRowStreamDecoderTag()));
        auto rows = reader->ReadUnversionedRowset(true);

        auto oldNameTableSize = Descriptor_.name_table_entries_size();
        YT_VERIFY(oldNameTableSize <= NameTable_->GetSize());

        Descriptor_.MergeFrom(descriptorDelta);
        auto newNameTableSize = Descriptor_.name_table_entries_size();

        IdMapping_.resize(newNameTableSize);
        for (int id = oldNameTableSize; id < newNameTableSize; ++id) {
            const auto& name = Descriptor_.name_table_entries(id).name();
            auto mappedId = NameTable_->GetIdOrRegisterName(name);
            IdMapping_[id] = mappedId;
            HasNontrivialIdMapping_ |= (id != mappedId);
        }

        if (HasNontrivialIdMapping_) {
            for (auto row : rows) {
                auto mutableRow = TMutableUnversionedRow(row.ToTypeErasedRow());
                for (auto& value : mutableRow) {
                    auto newId = ApplyIdMapping(value, &IdMapping_);
                    if (newId < 0 || newId >= NameTable_->GetSize()) {
                        THROW_ERROR_EXCEPTION("Id mapping returned an invalid value %v for id %v: "
                            "expected a value in [0, %v) range",
                            newId,
                            value.Id,
                            NameTable_->GetSize());
                    }
                    value.Id = newId;
                }
            }
        }

        return CreateBatchFromUnversionedRows(std::move(rows));
    }

private:
    const TNameTablePtr NameTable_;

    NApi::NRpcProxy::NProto::TRowsetDescriptor Descriptor_;
    TNameTableToSchemaIdMapping IdMapping_;
    bool HasNontrivialIdMapping_ = false;
};

IRowStreamDecoderPtr CreateWireRowStreamDecoder(TNameTablePtr nameTable)
{
    return New<TWireRowStreamDecoder>(std::move(nameTable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy


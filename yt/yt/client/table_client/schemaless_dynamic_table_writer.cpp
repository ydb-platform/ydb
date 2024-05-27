#include "schemaless_dynamic_table_writer.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_writer.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NTransactionClient;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static auto Logger = NLogging::TLogger("SchemalessDynamicTableWriter");

////////////////////////////////////////////////////////////////////////////////

class TSchemalessDynamicTableWriter
    : public IUnversionedWriter
{
public:
    TSchemalessDynamicTableWriter(TYPath path, IClientPtr client)
        : Path_(std::move(path))
        , Client_(std::move(client))
    { }

    TFuture<void> GetReadyEvent() override
    {
        // NB: this writer is synchronous and throws no errors. It just logs
        // them instead. See TSchemalessDynamicTableWriter::Write().
        return VoidFuture;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        try {
            return DoWrite(rows);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Could not write to event log");
            return false;
        }
    }

    bool DoWrite(TRange<TUnversionedRow> rows)
    {
        TUnversionedRowsBuilder builder;
        for (auto row : rows) {
            builder.AddRow(row);
        }

        auto sharedRows = builder.Build();

        auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        transaction->WriteRows(Path_, NameTable_, sharedRows);
        WaitFor(transaction->Commit())
            .ThrowOnError();

        return true;
    }

    TFuture<void> Close() override
    {
        return VoidFuture;
    }

    const TTableSchemaPtr& GetSchema() const override
    {
        return Schema_;
    }

    const TNameTablePtr& GetNameTable() const override
    {
        return NameTable_;
    }

private:
    const TYPath Path_;
    const IClientPtr Client_;

    const TNameTablePtr NameTable_ = New<TNameTable>();
    const TTableSchemaPtr Schema_ = New<TTableSchema>();
};

////////////////////////////////////////////////////////////////////////////////

IUnversionedWriterPtr CreateSchemalessDynamicTableWriter(TYPath path, IClientPtr client)
{
    return New<TSchemalessDynamicTableWriter>(std::move(path), std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#include "schemaless_dynamic_table_writer.h"
#include "helpers.h"
#include "name_table.h"
#include "schema.h"
#include "unversioned_writer.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

namespace NYT::NTableClient {

using namespace NApi;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NTransactionClient;
using namespace NYPath;

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
        // NB(eshcherbin): This writer is synchronous and it's always ready.
        return VoidFuture;
    }

    bool Write(TRange<TUnversionedRow> rows) override
    {
        TUnversionedRowsBuilder builder;
        builder.ReserveRows(std::ssize(rows));
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

    std::optional<TMD5Hash> GetDigest() const override
    {
        return std::nullopt;
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

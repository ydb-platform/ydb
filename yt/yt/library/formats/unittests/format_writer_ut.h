#pragma once

#include <yt/yt/library/formats/format.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFormats {
namespace {

////////////////////////////////////////////////////////////////////////////////

void TestNameTableExpansion(ISchemalessFormatWriterPtr writer, NTableClient::TNameTablePtr nameTable)
{
    // We write five rows, on each iteration we double number of
    // columns in the NameTable.
    for (int iteration = 0; iteration < 5; ++iteration) {
        NTableClient::TUnversionedOwningRowBuilder row;
        for (int index = 0; index < (1 << iteration); ++index) {
            auto key = "Column" + ToString(index);
            auto value = "Value" + ToString(index);
            int columnId = nameTable->GetIdOrRegisterName(key);
            row.AddValue(NTableClient::MakeUnversionedStringValue(value, columnId));
        }
        auto completeRow = row.FinishRow();
        EXPECT_EQ(true, writer->Write({completeRow.Get()}));
    }
    writer->Close()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFormats

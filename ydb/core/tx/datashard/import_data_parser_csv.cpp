#ifndef KIKIMR_DISABLE_S3_OPS

#include "import_data_parser.h"

#include <ydb/core/io_formats/ydb_dump/csv_ydb_dump.h>
#include <ydb/core/scheme/scheme_types_proto.h>

#include <util/string/builder.h>

namespace NKikimr::NDataShard {

namespace {

class TCsvDataParser final : public IDataParser {
public:
    bool Configure(
        const TTableInfo& tableInfo,
        const NKikimrSchemeOp::TTableDescription& scheme,
        TString& error) override
    {
        Y_UNUSED(error);
        ColumnOrderTypes.clear();
        ColumnOrderTypes.reserve(scheme.GetColumns().size());

        for (auto&& column : scheme.GetColumns()) {
            auto typeInfoMod = NScheme::TypeInfoModFromProtoColumnType(
                column.GetTypeId(),
                column.HasTypeInfo() ? &column.GetTypeInfo() : nullptr);
            ColumnOrderTypes.emplace_back(tableInfo.KeyOrder(column.GetName()), typeInfoMod.TypeInfo);
        }

        return true;
    }

    bool ParseBlock(
        TStringBuf data,
        TMemoryPool& pool,
        ui64& pendingBytes,
        ui64& pendingRows,
        const TAddRowFn& addRow,
        TString& error) override
    {
        while (data) {
            TStringBuf line = data.NextTok('\n');
            const TStringBuf origLine = line;

            if (!line) {
                if (data) {
                    continue;
                }

                return true;
            }

            pool.Clear();

            TVector<TCell> keys;
            TVector<TCell> values;
            TString strError;

            if (!NFormats::TYdbDump::ParseLine(line, ColumnOrderTypes, pool, keys, values, strError, pendingBytes)) {
                error = TStringBuilder() << strError << " on line: " << origLine;
                return false;
            }

            addRow(keys, values);
            ++pendingRows;
        }

        return true;
    }

private:
    std::vector<std::pair<i32, NScheme::TTypeInfo>> ColumnOrderTypes;
};

} // anonymous namespace

IDataParser::TPtr CreateCsvDataParser() {
    return MakeHolder<TCsvDataParser>();
}

} // namespace NKikimr::NDataShard

#endif // KIKIMR_DISABLE_S3_OPS

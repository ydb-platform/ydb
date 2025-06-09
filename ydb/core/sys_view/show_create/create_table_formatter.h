#pragma once

#include "formatters_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/tx/columnshard/engines/scheme/defaults/protos/data.pb.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/stream/str.h>

namespace NKikimr {
namespace NSysView {

class TCreateTableFormatter {
public:
    TCreateTableFormatter()
        : Alloc(__LOCATION__)
    {
        Alloc.Release();
    }

    ~TCreateTableFormatter()
    {
        Alloc.Acquire();
    }

    TFormatResult Format(const TString& tablePath, const TString& fullPath, const NKikimrSchemeOp::TTableDescription& tableDesc, bool temporary,
        const THashMap<TString, THolder<NKikimrSchemeOp::TPersQueueGroupDescription>>& persQueues,
        const THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>>& sequences);
    TFormatResult Format(const TString& tablePath, const TString& fullPath, const NKikimrSchemeOp::TColumnTableDescription& tableDesc, bool temporary);

private:
    void Format(const NKikimrSchemeOp::TColumnDescription& columnDesc);
    bool Format(const NKikimrSchemeOp::TFamilyDescription& familyDesc);
    bool Format(const NKikimrSchemeOp::TPartitioningPolicy& policy, ui32 shardsToCreate, TString& del, bool needWith);

    void Format(const TString& tablePath, const NKikimrSchemeOp::TCdcStreamDescription& cdcStream,
        const THashMap<TString, THolder<NKikimrSchemeOp::TPersQueueGroupDescription>>& persQueues, ui32 firstColumnTypeId);
    void Format(const TString& fullTablePath, const NKikimrSchemeOp::TSequenceDescription& sequence, const THashMap<TPathId, THolder<NSequenceProxy::TEvSequenceProxy::TEvGetSequenceResult>>& sequences);
    void FormatIndexImplTable(const TString& tablePath, const TString& indexName, const NKikimrSchemeOp::TTableDescription& indexImplDesc);

    void Format(const Ydb::Table::TableIndex& index);
    bool Format(const Ydb::Table::ExplicitPartitions& explicitPartitions, TString& del, bool needWith);
    bool Format(const Ydb::Table::ReadReplicasSettings& readReplicasSettings, TString& del, bool needWith);
    bool Format(const Ydb::Table::TtlSettings& ttlSettings, TString& del, bool needWith);

    void Format(ui64 expireAfterSeconds, std::optional<TString> storage = std::nullopt);

    void Format(const NKikimrSchemeOp::TOlapColumnDescription& olapColumnDesc);
    void Format(const NKikimrSchemeOp::TColumnTableSharding& tableSharding);
    void Format(const NKikimrSchemeOp::TColumnDataLifeCycle& ttlSettings);

    void FormatAlterColumn(const TString& fullPath, const NKikimrSchemeOp::TOlapColumnDescription& columnDesc, const NKikimrSchemeOp::TFamilyDescription* family);
    void FormatUpsertIndex(const TString& fullPath, const NKikimrSchemeOp::TOlapIndexDescription& indexDesc,
        const std::map<ui32, const NKikimrSchemeOp::TOlapColumnDescription*>& columns);
    void FormatUpsertOptions(const TString& fullPath, const NKikimrSchemeOp::TColumnTableSchemeOptions& options);

    void Format(const Ydb::TypedValue& value, bool isPartition = false);
    void FormatValue(NYdb::TValueParser& parser, bool isPartition = false, TString del = "");
    void FormatPrimitive(NYdb::TValueParser& parser);

    TString ValueToString(const NKikimrColumnShardColumnDefaults::TColumnDefault& defaultValue);

    TStringStream Stream;
    NMiniKQL::TScopedAlloc Alloc;
};

} // NSysView
} // NKikimr

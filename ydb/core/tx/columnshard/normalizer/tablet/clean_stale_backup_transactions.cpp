#include "clean_stale_backup_transactions.h"

#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <type_traits>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap {

namespace {

struct TStaleBackupTransaction {
    ui64 TxId;
    const char* Reason;
    TString TxInfoJson;
};

void ClearBackupSecrets(NKikimrSchemeOp::TBackupTask* task) {
    if (task->HasS3Settings()) {
        auto* settings = task->MutableS3Settings();
        settings->ClearAccessKey();
        settings->ClearSecretKey();
        if (settings->GetSecretableAccessKey().HasValue()) {
            settings->MutableSecretableAccessKey()->MutableValue()->ClearData();
        }
        if (settings->GetSecretableSecretKey().HasValue()) {
            settings->MutableSecretableSecretKey()->MutableValue()->ClearData();
        }
    }
    if (task->HasYTSettings()) {
        task->MutableYTSettings()->ClearToken();
    }
    if (task->HasEncryptionSettings()) {
        task->MutableEncryptionSettings()->ClearKey();
    }
}

template <class TRowset>
void WriteSanitizedTxBody(NJsonWriter::TBuf& json, const TRowset& rowset) {
    using TTxBody = NColumnShard::Schema::TxInfo::TxBody;
    json.WriteKey("TxBodyBase64");
    if (!rowset.template HaveValue<TTxBody>()) {
        json.WriteNull();
        return;
    }
    NKikimrTxColumnShard::TBackupTxBody body;
    if (!body.ParseFromString(rowset.template GetValue<TTxBody>())) {
        // Never fall back to logging unparsed data that may contain secrets.
        json.WriteNull();
        json.WriteKey("TxBodyParseError").WriteBool(true);
        return;
    }
    if (body.HasBackupTask()) {
        ClearBackupSecrets(body.MutableBackupTask());
    }
    json.WriteString(Base64Encode(body.SerializeAsString()));
}

template <class TColumn, class TRowset>
void WriteTxInfoColumn(NJsonWriter::TBuf& json, const TRowset& rowset, const TStringBuf name) {
    json.WriteKey(name);
    if (!rowset.template HaveValue<TColumn>()) {
        json.WriteNull();
    } else {
        const auto value = rowset.template GetValue<TColumn>();
        if constexpr (std::is_same_v<typename TColumn::Type, TString>) {
            json.WriteString(Base64Encode(value));
        } else if constexpr (std::is_same_v<typename TColumn::Type, TActorId>) {
            json.BeginObject();
            json.WriteKey("RawX1").WriteULongLong(value.RawX1());
            json.WriteKey("RawX2").WriteULongLong(value.RawX2());
            json.EndObject();
        } else {
            json.WriteULongLong(value);
        }
    }
}

template <class TRowset>
TString SerializeTxInfo(const TRowset& rowset, const ui64 tabletId) {
    using TTxInfo = NColumnShard::Schema::TxInfo;
    // Preserve TxBody except for secrets. Keep nulls and both raw ActorId words;
    // only binary columns (TxBody and SeqNo) are encoded in base64.
    NJsonWriter::TBuf json;
    json.BeginObject();
    json.WriteKey("Version").WriteInt(1);
    json.WriteKey("TabletId").WriteULongLong(tabletId);
    json.WriteKey("TxInfo").BeginObject();
    WriteTxInfoColumn<TTxInfo::TxId>(json, rowset, "TxId");
    WriteTxInfoColumn<TTxInfo::TxKind>(json, rowset, "TxKind");
    WriteSanitizedTxBody(json, rowset);
    WriteTxInfoColumn<TTxInfo::MaxStep>(json, rowset, "MaxStep");
    WriteTxInfoColumn<TTxInfo::PlanStep>(json, rowset, "PlanStep");
    WriteTxInfoColumn<TTxInfo::Source>(json, rowset, "Source");
    WriteTxInfoColumn<TTxInfo::Cookie>(json, rowset, "Cookie");
    WriteTxInfoColumn<TTxInfo::SeqNo>(json, rowset, "SeqNoBase64");
    json.EndObject();
    json.EndObject();
    return json.Str();
}

class TCleanStaleBackupTransactionsChanges: public INormalizerChanges {
private:
    const std::vector<TStaleBackupTransaction> Transactions;

public:
    explicit TCleanStaleBackupTransactionsChanges(std::vector<TStaleBackupTransaction>&& transactions)
        : Transactions(std::move(transactions))
    {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& tx : Transactions) {
            YDB_LOG_WARN("",
                {"normalizer", TCleanStaleBackupTransactionsNormalizer::GetClassNameStatic()},
                {"event", "removing_stale_backup_transaction"},
                {"txId", tx.TxId},
                {"reason", tx.Reason},
                {"txInfoJson", tx.TxInfoJson});
            NColumnShard::Schema::EraseTxInfo(db, tx.TxId);
        }
        return true;
    }

    ui64 GetSize() const override {
        return Transactions.size();
    }

    TString DebugString() const override {
        return TStringBuilder() << "stale_backup_transactions=" << Transactions.size();
    }
};

}   // namespace

TConclusion<std::vector<INormalizerTask::TPtr>> TCleanStaleBackupTransactionsNormalizer::DoInit(
    const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    THashSet<ui64> completedTxIds;
    THashSet<ui64> tablePathIds;
    {
        auto rowset = db.Table<Schema::TableInfoV1>()
                          .Select<Schema::TableInfoV1::SchemeShardLocalPathId, Schema::TableInfoV1::LastCompletedBackupTransaction>();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("cannot read TableInfoV1");
        }
        while (!rowset.EndOfSet()) {
            tablePathIds.insert(rowset.GetValue<Schema::TableInfoV1::SchemeShardLocalPathId>());
            if (rowset.HaveValue<Schema::TableInfoV1::LastCompletedBackupTransaction>()) {
                const TString data = rowset.GetValue<Schema::TableInfoV1::LastCompletedBackupTransaction>();
                NKikimrTxColumnShard::TCompletedBackupTransaction completedTx;
                if (completedTx.ParseFromString(data) && completedTx.HasTxId() && completedTx.HasOpResult()) {
                    completedTxIds.insert(completedTx.GetTxId());
                }
            }
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("cannot read TableInfoV1");
            }
        }
    }
    {
        // Older tablets can have tables represented only in TableInfo. BackupTask::TableId
        // is a SchemeShard path id, which is not necessarily the internal PathId.
        auto rowset = db.Table<Schema::TableInfo>().Select<Schema::TableInfo::PathId, Schema::TableInfo::SchemeShardLocalPathId>();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("cannot read TableInfo");
        }
        while (!rowset.EndOfSet()) {
            tablePathIds.insert(rowset.HaveValue<Schema::TableInfo::SchemeShardLocalPathId>()
                                    ? rowset.GetValue<Schema::TableInfo::SchemeShardLocalPathId>()
                                    : rowset.GetValue<Schema::TableInfo::PathId>());
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("cannot read TableInfo");
            }
        }
    }

    // A missing background session is not proof of completion. Remove only transactions
    // with a persisted result or a missing table, keeping results for TEvNotifyTxCompletion.
    std::vector<TStaleBackupTransaction> transactions;
    auto rowset = db.Table<Schema::TxInfo>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read TxInfo");
    }
    while (!rowset.EndOfSet()) {
        const ui64 txId = rowset.GetValue<Schema::TxInfo::TxId>();
        if (rowset.HaveValue<Schema::TxInfo::TxKind>() && rowset.GetValue<Schema::TxInfo::TxKind>() == NKikimrTxColumnShard::TX_KIND_BACKUP) {
            if (completedTxIds.contains(txId)) {
                transactions.push_back({ txId, "completed", SerializeTxInfo(rowset, TabletId) });
            } else {
                NKikimrTxColumnShard::TBackupTxBody body;
                if (body.ParseFromString(rowset.GetValue<Schema::TxInfo::TxBody>()) && body.HasBackupTask() &&
                    body.GetBackupTask().HasTableId() && body.GetBackupTask().GetTableId() &&
                    !tablePathIds.contains(body.GetBackupTask().GetTableId())) {
                    transactions.push_back({ txId, "table_removed", SerializeTxInfo(rowset, TabletId) });
                }
            }
        }
        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read TxInfo");
        }
    }

    constexpr size_t TransactionsPerTask = 1000;
    std::vector<INormalizerTask::TPtr> tasks;
    for (size_t from = 0; from < transactions.size(); from += TransactionsPerTask) {
        const size_t to = std::min(transactions.size(), from + TransactionsPerTask);
        std::vector<TStaleBackupTransaction> batch(transactions.begin() + from, transactions.begin() + to);
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TCleanStaleBackupTransactionsChanges>(std::move(batch))));
    }
    return tasks;
}

}   // namespace NKikimr::NOlap

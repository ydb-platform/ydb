#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}

namespace NKikimr::NOlap {

    class TChunksNormalizer : public TNormalizationController::INormalizerComponent {
    public:

        static TString GetClassNameStatic() {
            return ::ToString(ENormalizerSequentialId::Chunks);
        }

        virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
            return ENormalizerSequentialId::Chunks;
        }

        virtual TString GetClassName() const override {
            return GetClassNameStatic();
        }

        class TNormalizerResult;

        class TKey {
            YDB_READONLY(ui64, Index, 0);
            YDB_READONLY(ui64, Granule, 0);
            YDB_READONLY(ui64, ColumnIdx, 0);
            YDB_READONLY(ui64, PlanStep, 0);
            YDB_READONLY(ui64, TxId, 0);
            YDB_READONLY(ui64, Portion, 0);
            YDB_READONLY(ui64, Chunk, 0);

        public:
            template <class TRowset>
            void Load(TRowset& rowset) {
                using namespace NColumnShard;
                Index = rowset.template GetValue<Schema::IndexColumns::Index>();
                Granule = rowset.template GetValue<Schema::IndexColumns::Granule>();
                ColumnIdx = rowset.template GetValue<Schema::IndexColumns::ColumnIdx>();
                PlanStep = rowset.template GetValue<Schema::IndexColumns::PlanStep>();
                TxId = rowset.template GetValue<Schema::IndexColumns::TxId>();
                Portion = rowset.template GetValue<Schema::IndexColumns::Portion>();
                Chunk = rowset.template GetValue<Schema::IndexColumns::Chunk>();
            }

            bool operator<(const TKey& other) const {
                return std::make_tuple(Portion, Chunk, ColumnIdx) < std::make_tuple(other.Portion, other.Chunk, other.ColumnIdx);
            }
        };

        class TUpdate {
            YDB_ACCESSOR(ui64, NumRows, 0);
            YDB_ACCESSOR(ui64, RawBytes, 0);
        };

        class TChunkInfo {
            YDB_READONLY_DEF(TKey, Key);
            TColumnChunkLoadContext CLContext;
            ISnapshotSchema::TPtr Schema;

            YDB_ACCESSOR_DEF(TUpdate, Update);
        public:
            template <class TSource>
            TChunkInfo(TKey&& key, const TSource& rowset, const IBlobGroupSelector* dsGroupSelector)
                : Key(std::move(key))
                , CLContext(rowset, dsGroupSelector)
            {}

            ui32 GetRecordsCount() const {
                return CLContext.GetMetaProto().GetNumRows();
            }

            const TBlobRange& GetBlobRange() const {
                return CLContext.GetBlobRange();
            }

            const NKikimrTxColumnShard::TIndexColumnMeta& GetMetaProto() const {
                return CLContext.GetMetaProto();
            }

            bool NormalizationRequired() const {
                return !CLContext.GetMetaProto().HasNumRows() || !CLContext.GetMetaProto().HasRawBytes();
            }

            std::shared_ptr<TColumnLoader> GetLoader() const {
                return Schema->GetColumnLoaderVerified(Key.GetColumnIdx());
            }
            void InitSchema(const NColumnShard::TTablesManager& tm);

            bool operator<(const TChunkInfo& other) const {
                return Key < other.Key;
            }
        };

        static inline INormalizerComponent::TFactory::TRegistrator<TChunksNormalizer> Registrator = INormalizerComponent::TFactory::TRegistrator<TChunksNormalizer>(GetClassNameStatic());
    public:
        TChunksNormalizer(const TNormalizationController::TInitContext& info)
            : DsGroupSelector(info.GetStorageInfo())
        {}

        virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

    private:
        NColumnShard::TBlobGroupSelector DsGroupSelector;
    };
}

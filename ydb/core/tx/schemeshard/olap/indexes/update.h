#pragma once
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract.h>

namespace NKikimr::NSchemeShard {

    class TOlapIndexUpsert {
    private:
        YDB_READONLY_DEF(TString, Name);
        YDB_READONLY_DEF(TString, TypeName);
    protected:
        NBackgroundTasks::TInterfaceProtoContainer<NOlap::NIndexes::IIndexMeta> IndexConstructor;
    public:
        TOlapIndexUpsert() = default;

        const NBackgroundTasks::TInterfaceProtoContainer<NOlap::NIndexes::IIndexMeta>& GetIndexConstructor() const {
            return IndexConstructor;
        }

        bool ParseFromRequest(const NKikimrSchemeOp::TOlapIndexDescription& columnSchema, IErrorCollector& errors);
        void ParseFromLocalDB(const NKikimrSchemeOp::TOlapIndexDescription& columnSchema);
        void Serialize(NKikimrSchemeOp::TOlapIndexDescription& columnSchema) const;
    };

    class TOlapIndexesUpdate {
    private:
        YDB_READONLY_DEF(TVector<TOlapIndexUpsert>, UpsertIndexes);
        YDB_READONLY_DEF(TSet<TString>, DropIndexes);
    public:
        bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors);
        bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
    };
}

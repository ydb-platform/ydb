#pragma once
#include <ydb/services/bg_tasks/abstract/interface.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/constructor.h>

namespace NKikimr::NSchemeShard {

    class TOlapIndexUpsert {
    private:
        YDB_READONLY_DEF(TString, Name);
        YDB_READONLY_DEF(TString, TypeName);
        YDB_READONLY_DEF(std::optional<TString>, StorageId);
    protected:
        NBackgroundTasks::TInterfaceProtoContainer<NOlap::NIndexes::IIndexMetaConstructor> IndexConstructor;
    public:
        TOlapIndexUpsert() = default;

        const NBackgroundTasks::TInterfaceProtoContainer<NOlap::NIndexes::IIndexMetaConstructor>& GetIndexConstructor() const {
            return IndexConstructor;
        }

        bool DeserializeFromProto(const NKikimrSchemeOp::TOlapIndexRequested& requestedProto);
        void SerializeToProto(NKikimrSchemeOp::TOlapIndexRequested& requestedProto) const;
    };

    class TOlapIndexesUpdate {
    private:
        YDB_READONLY_DEF(TVector<TOlapIndexUpsert>, UpsertIndexes);
        YDB_READONLY_DEF(TSet<TString>, DropIndexes);
    public:
        bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
    };
}

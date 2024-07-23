#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/vector.h>

namespace NKikimr::NChangeExchange {

template<typename TChangeRecord>
class IChangeSenderPartitioner {
public:
    virtual ~IChangeSenderPartitioner() = default;

    virtual ui64 ResolvePartitionId(const typename TChangeRecord::TPtr& record) const = 0;
};


ui64 ResolveSchemaBoundaryPartitionId(const NKikimr::TKeyDesc& keyDesc, TConstArrayRef<TCell> key);

template<typename TChangeRecord>
class SchemaBoundaryPartitioner final : public IChangeSenderPartitioner<TChangeRecord> {
public:
    SchemaBoundaryPartitioner(const NKikimr::TKeyDesc& keyDesc)
        : KeyDesc(keyDesc) {
    }

    ui64 ResolvePartitionId(const typename TChangeRecord::TPtr& record) const override {
        return ResolveSchemaBoundaryPartitionId(KeyDesc, record->GetKey());
    }

private:
    const NKikimr::TKeyDesc& KeyDesc;
};


template<typename TChangeRecord>
IChangeSenderPartitioner<TChangeRecord>* CreateSchemaBoundaryPartitioner(const NKikimr::TKeyDesc& keyDesc) {
    return new SchemaBoundaryPartitioner<TChangeRecord>(keyDesc);
}


} // NKikimr::NChangeExchange

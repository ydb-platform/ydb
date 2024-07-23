#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/vector.h>

namespace NKikimr::NChangeExchange {

template<typename TChangeRecord>
class IChangeSenderChooser {
public:
    virtual ~IChangeSenderChooser() = default;

    virtual ui64 ResolvePartitionId(const typename TChangeRecord::TPtr& record) const = 0;
};


ui64 ResolveSchemaBoundaryPartitionId(NKikimr::TKeyDesc* keyDesc, TConstArrayRef<TCell> key);

template<typename TChangeRecord>
class SchemaBoundaryChooser final : public IChangeSenderChooser<TChangeRecord> {
public:
    SchemaBoundaryChooser(NKikimr::TKeyDesc* keyDesc)
        : KeyDesc(keyDesc) {
    }

    ui64 ResolvePartitionId(const typename TChangeRecord::TPtr& record) const override {
        return ResolveSchemaBoundaryPartitionId(KeyDesc, record->GetKey());
    }

private:
    NKikimr::TKeyDesc* KeyDesc;
};


template<typename TChangeRecord>
IChangeSenderChooser<TChangeRecord>* CreateSchemaBoundaryChooser(NKikimr::TKeyDesc* keyDesc) {
    return new SchemaBoundaryChooser<TChangeRecord>(keyDesc);
}


} // NKikimr::NChangeExchange

#pragma once
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

class TOlapColumnFamlilyDiff {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(NKikimrSchemeOp::EColumnCodec, Codec);

public:
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnFamilyDiff& diffColumnFamily, IErrorCollector& errors);
};

class TOlapColumnFamlilyAdd {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(TMaybe<TString>, Data);
    YDB_READONLY_DEF(NKikimrSchemeOp::EColumnCodec, Codec);
    YDB_READONLY_DEF(TMaybe<i32>, CodecLevel);

public:
    bool ParseFromRequest(const NKikimrSchemeOp::TFamilyDescription& columnFamily, IErrorCollector& errors);
    void ParseFromLocalDB(const NKikimrSchemeOp::TFamilyDescription& columnFamily);
    void Serialize(NKikimrSchemeOp::TFamilyDescription& columnSchema) const;
    bool ApplyDiff(const TOlapColumnFamlilyDiff& diffColumn, IErrorCollector& errors);
};

class TOlapColumnFamiliesUpdate {
private:
    YDB_READONLY_DEF(TVector<TOlapColumnFamlilyAdd>, AddColumnFamilies);
    // YDB_READONLY_DEF(TSet<TString>, DropColumnFamily); Delete Column Family ???
    YDB_READONLY_DEF(TVector<TOlapColumnFamlilyAdd>, AlterColumnFamily);

public:
    bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors);
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
};

}  // namespace NKikimr::NSchemeShard

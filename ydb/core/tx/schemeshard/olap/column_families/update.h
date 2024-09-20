#pragma once
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/schemeshard/olap/common/common.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NSchemeShard {

class TOlapColumnFamilyHelper {
protected:
    [[nodiscard]] NKikimr::TConclusion<std::shared_ptr<NArrow::NSerialization::ISerializer>> ParseSerializer(
        const NKikimrSchemeOp::TOlapColumn::TSerializer& serializer);
};

class TOlapColumnFamlilyDiff: public TOlapColumnFamilyHelper {
private:
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(NArrow::NSerialization::TSerializerContainer, SerializerContainer);

public:
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnFamilyDiff& diffColumnFamily, IErrorCollector& errors);
};

class TOlapColumnFamlilyAdd: public TOlapColumnFamilyHelper {
private:
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(NArrow::NSerialization::TSerializerContainer, SerializerContainer);

public:
    bool ParseFromRequest(const NKikimrSchemeOp::TOlapColumnFamily& columnFamily, IErrorCollector& errors);
    void ParseFromLocalDB(const NKikimrSchemeOp::TOlapColumnFamily& columnFamily);
    void Serialize(NKikimrSchemeOp::TOlapColumnFamily& columnSchema) const;
    bool ApplyDiff(const TOlapColumnFamlilyDiff& diffColumn, IErrorCollector& errors);
};

class TOlapColumnFamiliesUpdate {
private:
    YDB_READONLY_DEF(TVector<TOlapColumnFamlilyAdd>, AddColumnFamilies);
    // YDB_READONLY_DEF(TSet<TString>, DropColumnFamily); Delete Column Family ???
    YDB_READONLY_DEF(TVector<TOlapColumnFamlilyDiff>, AlterColumnFamily);

public:
    bool Parse(const NKikimrSchemeOp::TColumnTableSchema& tableSchema, IErrorCollector& errors);
    bool Parse(const NKikimrSchemeOp::TAlterColumnTableSchema& alterRequest, IErrorCollector& errors);
};

}

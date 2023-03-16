#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/abstract/kqp_common.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/public/api/protos/ydb_value.pb.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <library/cpp/object_factory/object_factory.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <util/string/cast.h>
#include "extractor/abstract.h"
#include "extractor/container.h"

namespace NKikimr::NMetadata::NCSIndex {

class TObject: public NModifications::TObject<TObject> {
private:
    YDB_READONLY_DEF(TString, IndexId);

    YDB_READONLY_DEF(TString, TablePath);
    YDB_READONLY_FLAG(Active, false);
    YDB_READONLY_FLAG(Delete, false);
    YDB_READONLY_DEF(TInterfaceContainer<IIndexExtractor>, Extractor);
public:

    bool TryProvideTtl(const NKikimrSchemeOp::TColumnTableDescription& csDescription, Ydb::Table::CreateTableRequest* cRequest);

    TString DebugString() const {
        return TablePath + ";" + IndexId + ";" + Extractor.DebugString() + ";" + ::ToString(ActiveFlag) + ";" + ::ToString(DeleteFlag);
    }

    TObject() = default;
    static std::optional<TObject> Build(const TString& tablePath, const TString& indexId, IIndexExtractor::TPtr extractor);

    TString GetIndexTablePath() const;

    TString GetUniqueId() const {
        return TablePath + "/" + IndexId;
    }

    class TDecoder: public NInternal::TDecoderBase {
    private:
        YDB_ACCESSOR(i32, IndexIdIdx, -1);
        YDB_ACCESSOR(i32, TablePathIdx, -1);
        YDB_ACCESSOR(i32, ExtractorIdx, -1);
        YDB_ACCESSOR(i32, ActiveIdx, -1);
        YDB_ACCESSOR(i32, DeleteIdx, -1);
    public:
        static inline const TString IndexId = "indexId";
        static inline const TString TablePath = "tablePath";
        static inline const TString Extractor = "extractor";
        static inline const TString Active = "active";
        static inline const TString Delete = "delete";

        TDecoder(const Ydb::ResultSet& rawData) {
            IndexIdIdx = GetFieldIndex(rawData, IndexId);
            TablePathIdx = GetFieldIndex(rawData, TablePath);
            ExtractorIdx = GetFieldIndex(rawData, Extractor);
            ActiveIdx = GetFieldIndex(rawData, Active);
            DeleteIdx = GetFieldIndex(rawData, Delete);
        }
    };

    static IClassBehaviour::TPtr GetBehaviour();

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);

    NInternal::TTableRecord SerializeToRecord() const;

    static TString GetTypeId() {
        return "CS_EXT_INDEX";
    }
};

}

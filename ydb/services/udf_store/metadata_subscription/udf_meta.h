#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

namespace NKikimr::NUdfStore {

using namespace NMetadata;

enum class EUdfType {
    NATIVE_UNSAFE,
    WASM
};

class TUdfMeta: public NModifications::TObject<TUdfMeta> {
public:
    static inline const TString Md5ColName = "md5"; //Utf8 (PK)
    static inline const TString SizeColName = "size"; // Uint64
    static inline const TString NameColName = "name"; //Utf8 - informative
    static inline const TString TypeColName = "type"; //Utf8 (allowed values from EUdfType)
    static inline const TString ManifestColName = "manifest"; //Json

private:
    YDB_ACCESSOR_DEF(TString, Md5);
    YDB_ACCESSOR_DEF(ui64, Size);
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(EUdfType, Type);
    YDB_ACCESSOR_DEF(TString, Manifest);
public:
    static IClassBehaviour::TPtr GetBehaviour();
    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();

    // TDecoder maps table columns to field indices.
    // Note: Body is NOT a table column; it is stored in a KV tablet.
    class TDecoder: public NInternal::TDecoderBase {
        using TBase = NInternal::TDecoderBase;
    private:
        YDB_ACCESSOR(i32, Md5Idx, -1);
        YDB_ACCESSOR(i32, SizeIdx, -1);
        YDB_ACCESSOR(i32, NameIdx, -1);
        YDB_ACCESSOR(i32, TypeIdx, -1);
        YDB_ACCESSOR(i32, ManifestIdx, -1);

    public:
        TDecoder(const Ydb::ResultSet& rawData);
        using TBase::Read;
        bool Read(const i32 columnIdx, EUdfType& result, const Ydb::Value& r) const;
    };

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NInternal::TTableRecord SerializeToRecord() const;
    static TString GetTypeId() {
        return "UdfMeta";
    }
    TString SerializeToString() const;

    bool operator<(const TUdfMeta& other) const {
        return Md5 < other.Md5;
    }
    bool operator==(const TUdfMeta& other) const {
        return Md5 == other.Md5;
    }
};

} // namespace NKikimr::NUdfStore

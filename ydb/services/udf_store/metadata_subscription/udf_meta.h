#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

namespace NKikimr::NUdfStore {

enum class EUdfType {
    NATIVE_UNSAFE,
    WASM
};

enum class ECompileStatus {
    Pending,
    Compiling,
    Ready,
    Failed,
};

class TUdfMeta: public NMetadata::NModifications::TObject<TUdfMeta> {
public:
    static inline const TString Md5ColName = "md5"; //Utf8 (PK)
    static inline const TString SizeColName = "size"; // Uint64
    static inline const TString NameColName = "name"; //Utf8 - informative
    static inline const TString TypeColName = "type"; //Utf8 (allowed values from EUdfType)
    static inline const TString ManifestColName = "manifest"; //Json
    static inline const TString VersionColName = "version"; // Uint64
    static inline const TString CompileStatusColName = "compile_status"; // Utf8
    static inline const TString CompileErrorColName = "compile_error"; // Utf8

private:
    YDB_ACCESSOR_DEF(TString, Md5);
    YDB_ACCESSOR_DEF(ui64, Size);
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(EUdfType, Type);
    YDB_ACCESSOR_DEF(TString, Manifest);
    YDB_ACCESSOR_DEF(ui64, Version);
    YDB_ACCESSOR_DEF(ECompileStatus, CompileStatus);
    YDB_ACCESSOR_DEF(TString, CompileError);
public:
    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();

    // TDecoder maps table columns to field indices.
    // WASM body is stored in udf_store/wasm_source; native body in KV volume.
    class TDecoder: public NMetadata::NInternal::TDecoderBase {
        using TBase = NMetadata::NInternal::TDecoderBase;
    private:
        YDB_ACCESSOR(i32, Md5Idx, -1);
        YDB_ACCESSOR(i32, SizeIdx, -1);
        YDB_ACCESSOR(i32, NameIdx, -1);
        YDB_ACCESSOR(i32, TypeIdx, -1);
        YDB_ACCESSOR(i32, ManifestIdx, -1);
        YDB_ACCESSOR(i32, VersionIdx, -1);
        YDB_ACCESSOR(i32, CompileStatusIdx, -1);
        YDB_ACCESSOR(i32, CompileErrorIdx, -1);

    public:
        TDecoder(const Ydb::ResultSet& rawData);
        using TBase::Read;
        bool Read(const i32 columnIdx, EUdfType& result, const Ydb::Value& r) const;
        bool Read(const i32 columnIdx, ECompileStatus& result, const Ydb::Value& r) const;
    };

    static TString CompileStatusToString(ECompileStatus status);
    static bool CompileStatusFromString(TStringBuf value, ECompileStatus& result);

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;
    TString SerializeToString() const;

    bool operator<(const TUdfMeta& other) const {
        return Md5 < other.Md5;
    }
    bool operator==(const TUdfMeta& other) const {
        return Md5 == other.Md5;
    }
};

} // namespace NKikimr::NUdfStore

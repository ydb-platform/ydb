#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/base/appdata.h>

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>
#include <ydb/services/metadata/manager/preparation_controller.h>

#include <util/datetime/base.h>

namespace NKikimr::NUdfStore {

enum class EUdfType {
    NATIVE_UNSAFE,
    WASM,
    LIBRARY,
};

enum class ECompileStatus {
    Pending,
    Compiling,
    Ready,
    Failed,
};

class TUdfModule: public NMetadata::NModifications::TObject<TUdfModule> {
public:
    static inline const TString UidColName = "uid"; // Utf8 (PK)
    static inline const TString Md5ColName = "md5"; // Utf8
    static inline const TString SizeColName = "size"; // Uint64
    static inline const TString NameColName = "name"; // Utf8
    static inline const TString TypeColName = "type"; // Utf8 (EUdfType)
    static inline const TString ManifestColName = "manifest"; // Json
    static inline const TString VersionColName = "version"; // Uint64
    static inline const TString ChunkCountColName = "chunk_count"; // Uint64
    static inline const TString CompileStatusColName = "compile_status"; // Utf8
    static inline const TString CompileErrorColName = "compile_error"; // Utf8
    static inline const TString CreatedAtColName = "created_at"; // Timestamp
    static inline const TString CompileStartedAtColName = "compile_started_at"; // Timestamp
    static inline const TString CompileFinishedAtColName = "compile_finished_at"; // Timestamp

private:
    YDB_ACCESSOR_DEF(TString, Uid);
    YDB_ACCESSOR_DEF(TString, Md5);
    YDB_ACCESSOR_DEF(ui64, Size);
    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(EUdfType, Type);
    YDB_ACCESSOR_DEF(TString, Manifest);
    YDB_ACCESSOR_DEF(ui64, Version);
    YDB_ACCESSOR_DEF(ui64, ChunkCount);
    YDB_ACCESSOR_DEF(ECompileStatus, CompileStatus);
    YDB_ACCESSOR_DEF(TString, CompileError);
    YDB_ACCESSOR_DEF(TInstant, CreatedAt);
    YDB_ACCESSOR_DEF(TInstant, CompileStartedAt);
    YDB_ACCESSOR_DEF(TInstant, CompileFinishedAt);
public:
    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();

    class TDecoder: public NMetadata::NInternal::TDecoderBase {
        using TBase = NMetadata::NInternal::TDecoderBase;
    private:
        YDB_ACCESSOR(i32, UidIdx, -1);
        YDB_ACCESSOR(i32, Md5Idx, -1);
        YDB_ACCESSOR(i32, SizeIdx, -1);
        YDB_ACCESSOR(i32, NameIdx, -1);
        YDB_ACCESSOR(i32, TypeIdx, -1);
        YDB_ACCESSOR(i32, ManifestIdx, -1);
        YDB_ACCESSOR(i32, VersionIdx, -1);
        YDB_ACCESSOR(i32, ChunkCountIdx, -1);
        YDB_ACCESSOR(i32, CompileStatusIdx, -1);
        YDB_ACCESSOR(i32, CompileErrorIdx, -1);
        YDB_ACCESSOR(i32, CreatedAtIdx, -1);
        YDB_ACCESSOR(i32, CompileStartedAtIdx, -1);
        YDB_ACCESSOR(i32, CompileFinishedAtIdx, -1);

    public:
        TDecoder(const Ydb::ResultSet& rawData);
        using TBase::Read;
        bool Read(const i32 columnIdx, EUdfType& result, const Ydb::Value& r) const;
        bool Read(const i32 columnIdx, ECompileStatus& result, const Ydb::Value& r) const;
    };

    static TString TypeToString(EUdfType type);
    static bool TypeFromString(TStringBuf value, EUdfType& result);
    static TString CompileStatusToString(ECompileStatus status);
    static bool CompileStatusFromString(TStringBuf value, ECompileStatus& result);

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;
    TString SerializeToString() const;

    bool operator<(const TUdfModule& other) const {
        return Uid < other.Uid;
    }
    bool operator==(const TUdfModule& other) const {
        return Uid == other.Uid;
    }
};

} // namespace NKikimr::NUdfStore

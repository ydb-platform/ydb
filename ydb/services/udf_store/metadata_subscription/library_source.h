#pragma once

#include "udf_meta.h"

#include <ydb/services/metadata/abstract/decoder.h>
#include <ydb/services/metadata/manager/object.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore {

class TUdfLibrarySource: public NMetadata::NModifications::TObject<TUdfLibrarySource> {
public:
    static inline const TString NameColName = "name";
    static inline const TString Md5ColName = "md5";
    static inline const TString VersionColName = "version";
    static inline const TString SizeColName = "size";
    static inline const TString ChunkCountColName = "chunk_count";
    static inline const TString CompileStatusColName = "compile_status";
    static inline const TString CompileErrorColName = "compile_error";

    YDB_ACCESSOR_DEF(TString, Name);
    YDB_ACCESSOR_DEF(TString, Md5);
    YDB_ACCESSOR_DEF(ui64, Version);
    YDB_ACCESSOR_DEF(ui64, Size);
    YDB_ACCESSOR_DEF(ui64, ChunkCount);
    YDB_ACCESSOR_DEF(ECompileStatus, CompileStatus);
    YDB_ACCESSOR_DEF(TString, CompileError);

public:
    static NMetadata::IClassBehaviour::TPtr GetBehaviour();
    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();

    class TDecoder: public NMetadata::NInternal::TDecoderBase {
        using TBase = NMetadata::NInternal::TDecoderBase;
    private:
        YDB_ACCESSOR(i32, NameIdx, -1);
        YDB_ACCESSOR(i32, Md5Idx, -1);
        YDB_ACCESSOR(i32, VersionIdx, -1);
        YDB_ACCESSOR(i32, SizeIdx, -1);
        YDB_ACCESSOR(i32, ChunkCountIdx, -1);
        YDB_ACCESSOR(i32, CompileStatusIdx, -1);
        YDB_ACCESSOR(i32, CompileErrorIdx, -1);

    public:
        TDecoder(const Ydb::ResultSet& rawData);

        using TBase::Read;
        bool Read(const i32 columnIdx, ECompileStatus& result, const Ydb::Value& r) const;
    };

    bool DeserializeFromRecord(const TDecoder& decoder, const Ydb::Value& rawValue);
    NMetadata::NInternal::TTableRecord SerializeToRecord() const;
    TString SerializeToString() const;
};

} // namespace NKikimr::NUdfStore

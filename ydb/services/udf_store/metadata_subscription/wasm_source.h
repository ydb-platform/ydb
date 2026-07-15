#pragma once

#include <ydb/services/metadata/manager/object.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NUdfStore {

class TUdfWasmSource: public NMetadata::NModifications::TObject<TUdfWasmSource> {
public:
    static inline const TString Md5ColName = "md5";
    static inline const TString VersionColName = "version";
    static inline const TString BodyColName = "body";

    YDB_ACCESSOR_DEF(TString, Md5);
    YDB_ACCESSOR_DEF(ui64, Version);
    YDB_ACCESSOR_DEF(TString, Body);

public:
    static TVector<NKikimrSchemeOp::TColumnDescription> GetColumnDescription();
    static TVector<TString> GetPk();
};

} // namespace NKikimr::NUdfStore

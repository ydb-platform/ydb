#pragma once

#include <util/generic/strbuf.h>

namespace NKikimrReplication {
    enum TSchemaChange_TFamily_ECompression : int;
    enum TSchemaChange_TFamily_ECacheMode : int;
}

namespace NKikimrSchemeOp {
    class TColumnDescription;
    class TFamilyDescription;
    enum EColumnCodec : int;
}

class IOutputStream;

namespace NKikimr::NReplication {

using EFamilyCompression = NKikimrReplication::TSchemaChange_TFamily_ECompression;
using EFamilyCacheMode = NKikimrReplication::TSchemaChange_TFamily_ECacheMode;

inline constexpr TStringBuf DefaultFamilyName = "default";

TStringBuf GetColumnFamilyName(const NKikimrSchemeOp::TColumnDescription& column);
TStringBuf GetFamilyName(const NKikimrSchemeOp::TFamilyDescription& family);
NKikimrSchemeOp::EColumnCodec GetColumnCodec(const NKikimrSchemeOp::TFamilyDescription& family);

struct TFamilySettings {
    TStringBuf Media;
    EFamilyCompression Compression;
    EFamilyCacheMode CacheMode;

    static TFamilySettings FromProto(const NKikimrSchemeOp::TFamilyDescription* family);

    bool operator==(const TFamilySettings& other) const;
    void Out(IOutputStream& out) const;
};

EFamilyCompression CompressionFromString(TStringBuf value);
EFamilyCacheMode CacheModeFromString(TStringBuf value);

TStringBuf CompressionToString(EFamilyCompression value);
TStringBuf CacheModeToString(EFamilyCacheMode value);

bool IsValidCompression(EFamilyCompression value);
bool IsValidCacheMode(EFamilyCacheMode value);

}

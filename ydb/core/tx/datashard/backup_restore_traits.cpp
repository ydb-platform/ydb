#include "backup_restore_traits.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/hash.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NDataShard {
namespace NBackupRestoreTraits {

bool TryCodecFromTask(const NKikimrSchemeOp::TBackupTask& task, ECompressionCodec& codec) {
    if (!task.HasCompression()) {
        codec = ECompressionCodec::None;
        return true;
    }

    if (!TryFromString<ECompressionCodec>(task.GetCompression().GetCodec(), codec)) {
        return false;
    }

    if (codec == ECompressionCodec::Invalid) {
        return false;
    }

    return true;
}

ECompressionCodec CodecFromTask(const NKikimrSchemeOp::TBackupTask& task) {
    ECompressionCodec codec;
    Y_ENSURE(TryCodecFromTask(task, codec));
    return codec;
}

EDataFormat NextDataFormat(EDataFormat cur) {
    switch (cur) {
    case EDataFormat::Csv:
        return EDataFormat::Invalid;
    case EDataFormat::Invalid:
        return EDataFormat::Invalid;
    }
}

ECompressionCodec NextCompressionCodec(ECompressionCodec cur) {
    switch (cur) {
    case ECompressionCodec::None:
        return ECompressionCodec::Zstd;
    case ECompressionCodec::Zstd:
        return ECompressionCodec::Invalid;
    case ECompressionCodec::Invalid:
        return ECompressionCodec::Invalid;
    }
}

TString DataFileExtension(EDataFormat format, ECompressionCodec codec) {
    static THashMap<EDataFormat, TString> formats = {
        {EDataFormat::Csv, ".csv"},
    };

    static THashMap<ECompressionCodec, TString> codecs = {
        {ECompressionCodec::None, ""},
        {ECompressionCodec::Zstd, ".zst"},
    };

    auto fit = formats.find(format);
    Y_ENSURE(fit != formats.end(), "Unexpected format: " << format);

    auto cit = codecs.find(codec);
    Y_ENSURE(cit != codecs.end(), "Unexpected codec: " << codec);

    return Sprintf("%s%s", fit->second.c_str(), cit->second.c_str());
}

static TString AddEncryptedSuffix(TString name, bool encryptedBackup) {
    if (encryptedBackup) {
        name.append(".enc");
    }
    return name;
}

TString PermissionsKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("permissions.pb", encryptedBackup);
}

TString TopicKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("topic_description.pb", encryptedBackup);
}

TString ChangefeedKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("changefeed_description.pb", encryptedBackup);
}

TString SchemeKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("scheme.pb", encryptedBackup);
}

TString MetadataKeySuffix(bool encryptedBackup) {
    return AddEncryptedSuffix("metadata.json", encryptedBackup);
}

TString DataKeySuffix(ui32 n, EDataFormat format, ECompressionCodec codec, bool encryptedBackup) {
    const auto ext = DataFileExtension(format, codec);
    return AddEncryptedSuffix(Sprintf("data_%02d%s", n, ext.c_str()), encryptedBackup);
}

} // NBackupRestoreTraits
} // NDataShard
} // NKikimr

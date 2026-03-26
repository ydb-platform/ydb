#pragma once

#include "abstract.h"
#include "parsing.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/options.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow::NSerialization {

class TNativeSerializer: public ISerializer {
public:
    static TString GetClassNameStatic() {
        return "ARROW_SERIALIZER";
    }
private:
    arrow20::ipc::IpcWriteOptions Options;

    TConclusion<std::shared_ptr<arrow20::util::Codec>> BuildCodec(const arrow20::Compression::type& cType, const std::optional<ui32> level) const;
    static const inline TFactory::TRegistrator<TNativeSerializer> Registrator = TFactory::TRegistrator<TNativeSerializer>(GetClassNameStatic());

    static std::shared_ptr<arrow20::util::Codec> GetDefaultCodec() {
        return *arrow20::util::Codec::Create(arrow20::Compression::LZ4_FRAME);
    }

protected:
    virtual bool IsCompatibleForExchangeWithSameClass(const ISerializer& /*item*/) const override {
        return true;
    }

    virtual bool IsEqualToSameClass(const ISerializer& item) const override {
        auto& itemOptions = static_cast<const TNativeSerializer&>(item).Options;
        if (!!itemOptions.codec != !!Options.codec) {
            return false;
        }
        if (!itemOptions.codec) {
            return true;
        }
        if (itemOptions.codec->name() != Options.codec->name()) {
            return false;
        }
        if (itemOptions.codec->compression_level() != Options.codec->compression_level()) {
            return false;
        }
        return true;
    }
    virtual TString DoSerializeFull(const std::shared_ptr<arrow20::RecordBatch>& batch) const override;
    virtual TString DoSerializePayload(const std::shared_ptr<arrow20::RecordBatch>& batch) const override;
    virtual arrow20::Result<std::shared_ptr<arrow20::RecordBatch>> DoDeserialize(const TString& data) const override;
    virtual arrow20::Result<std::shared_ptr<arrow20::RecordBatch>> DoDeserialize(const TString& data, const std::shared_ptr<arrow20::Schema>& schema) const override;

    virtual TConclusionStatus DoDeserializeFromRequest(NYql::TFeaturesExtractor& features) override;

    static arrow20::ipc::IpcOptions BuildDefaultOptions() {
        arrow20::ipc::IpcWriteOptions options;
        options.use_threads = false;
        if (HasAppData()) {
            if (AppData()->ColumnShardConfig.HasDefaultCompression()) {
                arrow20::Compression::type codec = CompressionFromProto(AppData()->ColumnShardConfig.GetDefaultCompression()).value();
                if (AppData()->ColumnShardConfig.HasDefaultCompressionLevel()) {
                    options.codec = NArrow::TStatusValidator::GetValid(
                        arrow20::util::Codec::Create(codec, AppData()->ColumnShardConfig.GetDefaultCompressionLevel()));
                } else {
                    options.codec = NArrow::TStatusValidator::GetValid(arrow20::util::Codec::Create(codec));
                }
            } else {
                options.codec = GetDefaultCodec();
            }
        } else {
            options.codec = GetDefaultCodec();
        }
        return options;
    }

    virtual TConclusionStatus DoDeserializeFromProto(const NKikimrSchemeOp::TOlapColumn::TSerializer& proto) override;

    virtual void DoSerializeToProto(NKikimrSchemeOp::TOlapColumn::TSerializer& proto) const override;

public:
    static std::shared_ptr<ISerializer> GetUncompressed() {
        static std::shared_ptr<ISerializer> result =
            std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow20::Compression::UNCOMPRESSED);
        return result;
    }

    static std::shared_ptr<ISerializer> GetFast() {
        static std::shared_ptr<ISerializer> result =
            std::make_shared<NArrow::NSerialization::TNativeSerializer>(arrow20::Compression::LZ4_FRAME);
        return result;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    virtual bool IsHardPacker() const override {
        return Options.codec && Options.codec->compression_type() == arrow20::Compression::ZSTD && Options.codec->compression_level() > 3;
    }

    static arrow20::ipc::IpcOptions GetDefaultOptions() {
        arrow20::ipc::IpcWriteOptions options = BuildDefaultOptions();
        return options;
    }

    TNativeSerializer(const arrow20::Compression::type compressionType) {
        Options.use_threads = false;
        auto r = arrow20::util::Codec::Create(compressionType);
        AFL_VERIFY(r.ok());
        Options.codec = std::move(*r);
    }

    TNativeSerializer(const arrow20::ipc::IpcWriteOptions& options = GetDefaultOptions())
        : Options(options) {
        Options.use_threads = false;
    }

    TNativeSerializer(arrow20::MemoryPool* pool) {
        Options.use_threads = false;
        Options.memory_pool = pool;
    }

    arrow20::Compression::type GetCodecType() const {
        if (Options.codec) {
            return Options.codec->compression_type();
        }
        return arrow20::Compression::type::UNCOMPRESSED;
    }

    std::optional<i32> GetCodecLevel() const {
        if (Options.codec && arrow20::util::Codec::SupportsCompressionLevel(Options.codec->compression_type())) {
            return Options.codec->compression_level();
        }
        return {};
    }
};

}

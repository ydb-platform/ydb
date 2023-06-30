#include "file_log_writer.h"

#include "log_writer_detail.h"
#include "config.h"
#include "private.h"
#include "log.h"
#include "random_access_gzip.h"
#include "stream_output.h"
#include "compression.h"
#include "log_writer_factory.h"
#include "zstd_compression.h"
#include "formatter.h"

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NLogging {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);
static constexpr size_t BufferSize = 64_KB;

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter
    : public TStreamLogWriterBase
    , public IFileLogWriter
{
public:
    TFileLogWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString name,
        TFileLogWriterConfigPtr config,
        ILogWriterHost* host)
        : TStreamLogWriterBase(std::move(formatter), std::move(name))
        , Config_(std::move(config))
        , Host_(host)
    {
        Open();
    }

    void Reload() override
    {
        Close();
        Open();
    }

    const TString& GetFileName() const override
    {
        return Config_->FileName;
    }

    void CheckSpace(i64 minSpace) override
    {
        try {
            auto directoryName = NFS::GetDirectoryName(Config_->FileName);
            auto statistics = NFS::GetDiskSpaceStatistics(directoryName);
            if (statistics.AvailableSpace < minSpace) {
                if (!Disabled_.load(std::memory_order::acquire)) {
                    Disabled_ = true;
                    YT_LOG_ERROR("Log file disabled: not enough space available (FileName: %v, AvailableSpace: %v, MinSpace: %v)",
                        directoryName,
                        statistics.AvailableSpace,
                        minSpace);

                    Close();
                }
            } else {
                if (Disabled_.load(std::memory_order::acquire)) {
                    Reload(); // Reinitialize all descriptors.

                    YT_LOG_INFO("Log file enabled: space check passed (FileName: %v)",
                        Config_->FileName);
                    Disabled_ = false;
                }
            }
        } catch (const std::exception& ex) {
            Disabled_ = true;
            YT_LOG_ERROR(ex, "Log file disabled: space check failed (FileName: %v)",
                Config_->FileName);

            Close();
        }
    }

protected:
    IOutputStream* GetOutputStream() const noexcept override
    {
        if (Y_UNLIKELY(Disabled_.load(std::memory_order::acquire))) {
            return nullptr;
        }
        return OutputStream_.Get();
    }

    void OnException(const std::exception& ex) override
    {
        Disabled_ = true;
        YT_LOG_ERROR(ex, "Disabled log file (FileName: %v)",
            Config_->FileName);

        Close();
    }

private:
    const TFileLogWriterConfigPtr Config_;
    ILogWriterHost* const Host_;

    std::atomic<bool> Disabled_ = false;

    std::unique_ptr<TFile> File_;
    IStreamLogOutputPtr OutputStream_;


    void Open()
    {
        Disabled_ = false;
        try {
            NFS::MakeDirRecursive(NFS::GetDirectoryName(Config_->FileName));

            TFlags<EOpenModeFlag> openMode;
            if (Config_->EnableCompression) {
                openMode = OpenAlways|RdWr|CloseOnExec;
            } else {
                openMode = OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec;
            }

            File_.reset(new TFile(Config_->FileName, openMode));

            if (Config_->EnableCompression) {
                switch (Config_->CompressionMethod) {
                    case ECompressionMethod::Zstd:
                        OutputStream_ = New<TAppendableCompressedFile>(
                            *File_,
                            CreateZstdCompressionCodec(Config_->CompressionLevel),
                            Host_->GetCompressionInvoker(),
                            /*writeTruncateMessage*/ true);
                        break;

                    case ECompressionMethod::Gzip:
                        OutputStream_ = New<TRandomAccessGZipFile>(
                            *File_,
                            Config_->CompressionLevel);
                        break;

                    default:
                        YT_ABORT();
                }
            } else {
                OutputStream_ = New<TFixedBufferFileOutput>(
                    *File_,
                    BufferSize);
            }

            // Emit a delimiter for ease of navigation.
            if (File_->GetLength() > 0) {
                Formatter_->WriteLogReopenSeparator(GetOutputStream());
            }

            Formatter_->WriteLogStartEvent(GetOutputStream());

            ResetCurrentSegment(File_->GetLength());
        } catch (const std::exception& ex) {
            Disabled_ = true;
            YT_LOG_ERROR(ex, "Failed to open log file (FileName: %v)",
                Config_->FileName);

            Close();
        } catch (...) {
            YT_ABORT();
        }
    }

    void Close()
    {
        try {
            if (OutputStream_) {
                OutputStream_->Finish();
                OutputStream_.Reset();
            }

            if (File_) {
                File_->Close();
                File_.reset();
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to close log file; ignored (FileName: %v)",
                Config_->FileName);
        } catch (...) {
            YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileLogWriterPtr CreateFileLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    TString name,
    TFileLogWriterConfigPtr config,
    ILogWriterHost* host)
{
    return New<TFileLogWriter>(
        std::move(formatter),
        std::move(name),
        std::move(config),
        host);
}

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriterFactory
    : public ILogWriterFactory
{
public:
    void ValidateConfig(
        const NYTree::IMapNodePtr& configNode) override
    {
        ParseConfig(configNode);
    }

    ILogWriterPtr CreateWriter(
        std::unique_ptr<ILogFormatter> formatter,
        TString name,
        const NYTree::IMapNodePtr& configNode,
        ILogWriterHost* host) noexcept override
    {
        return CreateFileLogWriter(
            std::move(formatter),
            std::move(name),
            ParseConfig(configNode),
            host);
    }

private:
    static TFileLogWriterConfigPtr ParseConfig(const NYTree::IMapNodePtr& configNode)
    {
        return ConvertTo<TFileLogWriterConfigPtr>(configNode);
    }
};

////////////////////////////////////////////////////////////////////////////////

ILogWriterFactoryPtr GetFileLogWriterFactory()
{
    return LeakyRefCountedSingleton<TFileLogWriterFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

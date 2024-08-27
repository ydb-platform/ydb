#include "file_log_writer.h"

#include "log_writer_detail.h"
#include "config.h"
#include "private.h"
#include "log.h"
#include "random_access_gzip.h"
#include "stream_output.h"
#include "system_log_event_provider.h"
#include "compression.h"
#include "log_writer_factory.h"
#include "zstd_compression.h"
#include "formatter.h"

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NLogging {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, SystemLoggingCategoryName);
static constexpr size_t BufferSize = 64_KB;

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriter
    : public TStreamLogWriterBase
    , public IFileLogWriter
{
public:
    TFileLogWriter(
        std::unique_ptr<ILogFormatter> formatter,
        std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
        TString name,
        const TFileLogWriterConfigPtr& config,
        ILogWriterHost* host)
        : TStreamLogWriterBase(
            std::move(formatter),
            std::move(systemEventProvider),
            std::move(name),
            config)
        , Config_(config)
        , Host_(host)
        , DirectoryName_(NFS::GetDirectoryName(Config_->FileName))
        , FileNamePrefix_(NFS::GetFileName(Config_->FileName))
        , LastRotationTimestamp_(TInstant::Now())
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
        return FileName_;
    }

    void MaybeRotate() override
    {
        const auto& rotationPolicy = Config_->RotationPolicy;
        auto now = TInstant::Now();
        if ((!rotationPolicy->RotationPeriod || LastRotationTimestamp_ + *rotationPolicy->RotationPeriod > now) &&
            ((!rotationPolicy->MaxSegmentSize || File_->GetLength() < *rotationPolicy->MaxSegmentSize)))
        {
            return;
        }

        Close();
        Rotate();
        Open();
    }

    void CheckSpace(i64 minSpace) override
    {
        try {
            auto statistics = NFS::GetDiskSpaceStatistics(DirectoryName_);
            if (statistics.AvailableSpace < minSpace) {
                if (!Disabled_.load(std::memory_order::acquire)) {
                    Disabled_ = true;
                    YT_LOG_ERROR("Log file disabled: not enough space available (FileName: %v, AvailableSpace: %v, MinSpace: %v)",
                        DirectoryName_,
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

    const TString DirectoryName_;
    const TString FileNamePrefix_;
    TString FileName_;

    std::atomic<bool> Disabled_ = false;
    TInstant LastRotationTimestamp_;

    std::unique_ptr<TFile> File_;
    IStreamLogOutputPtr OutputStream_;


    void Open()
    {
        Disabled_ = false;
        try {
            LastRotationTimestamp_ = TInstant::Now();
            NFS::MakeDirRecursive(DirectoryName_);

            TFlags<EOpenModeFlag> openMode;
            if (Config_->EnableCompression) {
                openMode = OpenAlways|RdWr|CloseOnExec;
            } else {
                openMode = OpenAlways|ForAppend|WrOnly|Seq|CloseOnExec;
            }

            // Generate filename.
            FileName_ = Config_->FileName;
            if (Config_->UseTimestampSuffix) {
                FileName_ += "." + LastRotationTimestamp_.ToStringLocalUpToSeconds();
            }

            File_.reset(new TFile(FileName_, openMode));

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

            if (auto logStartEvent = SystemEventProvider_->GetStartLogEvent()) {
                Formatter_->WriteFormatted(GetOutputStream(), *logStartEvent);
            }

            ResetSegmentSize(File_->GetLength());
        } catch (const std::exception& ex) {
            Disabled_ = true;
            YT_LOG_ERROR(ex, "Failed to open log file (FileName: %v)",
                FileName_);

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
                FileName_);
        } catch (...) {
            YT_ABORT();
        }
    }

    void Rotate()
    {
        try {
            auto fileNames = ListFiles();
            auto count = GetFileCountToKeep(fileNames);
            for (int index = count; index < ssize(fileNames); ++index) {
                auto filePath = NFS::CombinePaths(DirectoryName_, fileNames[index]);
                YT_LOG_DEBUG("Remove log segment (FilePath: %v)", filePath);
                NFS::Remove(filePath);
            }
            fileNames.resize(count);

            RenameFiles(fileNames);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to rotate log files");
        } catch (...) {
            YT_ABORT();
        }
    }

    std::vector<TString> ListFiles() const
    {
        auto files = NFS::EnumerateFiles(DirectoryName_);
        std::erase_if(files, [&] (const TString& s) {
            return !s.StartsWith(FileNamePrefix_);
        });
        if (Config_->UseTimestampSuffix) {
            // Rotated files are suffixed with the date, decreasing with the age of file.
            std::sort(files.begin(), files.end(), std::greater<TString>());
        } else {
            // Rotated files are suffixed with the number, increasing with the age of file.
            std::sort(files.begin(), files.end());
        }
        return files;
    }

    int GetFileCountToKeep(const std::vector<TString>& fileNames) const
    {
        const auto& rotationPolicy = Config_->RotationPolicy;
        int filesToKeep = 0;
        i64 totalSize = 0;
        for (const auto& fileName : fileNames) {
            auto fileSize = NFS::GetPathStatistics(NFS::CombinePaths(DirectoryName_, fileName)).Size;
            if (totalSize + fileSize > rotationPolicy->MaxTotalSizeToKeep ||
                filesToKeep + 1 > rotationPolicy->MaxSegmentCountToKeep)
            {
                return filesToKeep;
            }
            ++filesToKeep;
            totalSize += fileSize;
        }
        return fileNames.size();
    }

    void RenameFiles(const std::vector<TString>& fileNames)
    {
        if (Config_->UseTimestampSuffix) {
            return;
        }

        int width = ToString(ssize(fileNames)).length();
        TString formatString = "%v.%0" + ToString(width) + "d";
        for (int index = ssize(fileNames); index > 0; --index) {
            auto newFileName = Format(TRuntimeFormat{formatString}, FileNamePrefix_, index);
            auto oldPath = NFS::CombinePaths(DirectoryName_, fileNames[index - 1]);
            auto newPath = NFS::CombinePaths(DirectoryName_, newFileName);
            YT_LOG_DEBUG("Rename log segment (OldFilePath: %v, NewFilePath: %v)", oldPath, newPath);
            NFS::Rename(oldPath, newPath);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IFileLogWriterPtr CreateFileLogWriter(
    std::unique_ptr<ILogFormatter> formatter,
    std::unique_ptr<ISystemLogEventProvider> systemEventProvider,
    TString name,
    TFileLogWriterConfigPtr config,
    ILogWriterHost* host)
{
    return New<TFileLogWriter>(
        std::move(formatter),
        std::move(systemEventProvider),
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
        auto config = ParseConfig(configNode);
        return CreateFileLogWriter(
            std::move(formatter),
            CreateDefaultSystemLogEventProvider(config),
            std::move(name),
            std::move(config),
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

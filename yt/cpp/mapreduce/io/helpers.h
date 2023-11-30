#pragma once

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TOptions>
struct TIOOptionsTraits;

template <>
struct TIOOptionsTraits<TFileReaderOptions>
{
    static constexpr const char* const ConfigName = "file_reader";
};
template <>
struct TIOOptionsTraits<TFileWriterOptions>
{
    static constexpr const char* const ConfigName = "file_writer";
};
template <>
struct TIOOptionsTraits<TTableReaderOptions>
{
    static constexpr const char* const ConfigName = "table_reader";
};
template <>
struct TIOOptionsTraits<TTableWriterOptions>
{
    static constexpr const char* const ConfigName = "table_writer";
};

template <class TOptions>
TNode FormIORequestParameters(const TOptions& options)
{
    TNode params = TNode::CreateMap();
    if (options.Config_) {
        params[TIOOptionsTraits<TOptions>::ConfigName] = *options.Config_;
    }
    return params;
}

template <class TOptions>
TNode FormIORequestParameters(
    const TRichYPath& path,
    const TOptions& options)
{
    auto params = PathToParamNode(path);
    if (options.Config_) {
        params[TIOOptionsTraits<TOptions>::ConfigName] = *options.Config_;
    }
    return params;
}

template <>
inline TNode FormIORequestParameters(
    const TRichYPath& path,
    const TFileReaderOptions& options)
{
    auto params = PathToParamNode(path);
    if (options.Config_) {
        params[TIOOptionsTraits<TTableReaderOptions>::ConfigName] = *options.Config_;
    }
    if (options.Offset_) {
        params["offset"] = *options.Offset_;
    }
    if (options.Length_) {
        params["length"] = *options.Length_;
    }
    return params;
}

static void AddWriterOptionsToNode(const TWriterOptions& options, TNode* node)
{
    if (options.EnableEarlyFinish_) {
        (*node)["enable_early_finish"] = *options.EnableEarlyFinish_;
    }
    if (options.UploadReplicationFactor_) {
        (*node)["upload_replication_factor"] = *options.UploadReplicationFactor_;
    }
    if (options.MinUploadReplicationFactor_) {
        (*node)["min_upload_replication_factor"] = *options.MinUploadReplicationFactor_;
    }
    if (options.DesiredChunkSize_) {
        (*node)["desired_chunk_size"] = *options.DesiredChunkSize_;
    }
}

template <>
inline TNode FormIORequestParameters(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    auto params = PathToParamNode(path);
    TNode fileWriter = TNode::CreateMap();
    if (options.Config_) {
        fileWriter = *options.Config_;
    }
    if (options.WriterOptions_) {
        AddWriterOptionsToNode(*options.WriterOptions_, &fileWriter);
    }
    if (fileWriter.Empty()) {
        AddWriterOptionsToNode(
            TWriterOptions()
                .EnableEarlyFinish(true)
                .UploadReplicationFactor(3)
                .MinUploadReplicationFactor(2),
            &fileWriter);
    }
    params[TIOOptionsTraits<TFileWriterOptions>::ConfigName] = fileWriter;
    if (options.ComputeMD5_) {
        params["compute_md5"] = *options.ComputeMD5_;
    }
    return params;
}

template <>
inline TNode FormIORequestParameters(
    const TRichYPath& path,
    const TTableWriterOptions& options)
{
    auto params = PathToParamNode(path);
    auto tableWriter = TConfig::Get()->TableWriter;
    if (options.Config_) {
        MergeNodes(tableWriter, *options.Config_);
    }
    if (options.WriterOptions_) {
        AddWriterOptionsToNode(*options.WriterOptions_, &tableWriter);
    }
    if (!tableWriter.Empty()) {
        params[TIOOptionsTraits<TTableWriterOptions>::ConfigName] = std::move(tableWriter);
    }
    return params;
}

////////////////////////////////////////////////////////////////////////////////

}

#include "ydb_topic_deferred_publish.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_common.h>
#include <ydb/public/lib/ydb_cli/common/duration.h>
#include <ydb/public/lib/ydb_cli/common/pretty_table.h>
#include <ydb/public/lib/ydb_cli/common/scheme_path_completer.h>
#include <ydb/public/lib/ydb_cli/topic/topic_write.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/deferred_publications.h>

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>

#include <openssl/sha.h>
#include <util/stream/file.h>
#include <util/string/hex.h>

namespace NYdb::NConsoleClient {

using NTopic::TDeferredPublishClient;

namespace {

void WriteJsonLine(const NJson::TJsonValue& value) {
    NJson::WriteJson(&Cout, &value, /*formatOutput*/ false);
    Cout << Endl;
}

} // namespace

TCommandExperimentalTopic::TCommandExperimentalTopic()
    : TClientCommandTree("topic", {}, "Topic commands with experimental deferred publish support")
{
    AddCommand(std::make_unique<TCommandExperimentalTopicWrite>());
    AddCommand(std::make_unique<TCommandTopicDeferredPublication>());
}

TCommandExperimentalTopicWrite::TCommandExperimentalTopicWrite()
    : TYdbCommand("write", {}, "Write to topic; supports deferred publication staging")
{
}

void TCommandExperimentalTopicWrite::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->SetFreeArgsNum(1);
    SetFreeArgTitle(0, "<topic-path>", "Topic path");
    SetSchemePathCompletionForTopics(config.Opts->GetOpts().GetFreeArgSpec(0));

    AddMessagingFormats(config, {
        EMessagingFormat::NewlineDelimited,
        EMessagingFormat::SingleMessage,
    });
    AddAllowedCodecs(config, AllowedCodecs);

    config.Opts->AddLongOption('d', "delimiter", "Delimiter to split messages")
        .Optional()
        .StoreResult(&Delimiter_);
    config.Opts->AddLongOption('f', "file", "File to read data from")
        .Optional()
        .StoreResult(&File_);
    config.Opts->AddLongOption("message-group-id", "Message group identifier")
        .Optional()
        .StoreResult(&MessageGroupId_);
    config.Opts->AddLongOption("partition-id", "Write to an exact partition")
        .Hidden()
        .Optional()
        .RequiredArgument("INDEX")
        .StoreResult(&PartitionId_);
    config.Opts->AddLongOption("init-seqno-timeout", "Max wait duration for initial seqno")
        .Optional()
        .Hidden()
        .StoreMappedResult(&MessagesWaitTimeout_, &ParseDurationSeconds);

    config.Opts->AddLongOption("deferred-int-publication-id", "Deferred publication int_publication_id from begin")
        .Optional()
        .RequiredArgument("UINT64")
        .StoreResult(&DeferredIntPublicationId_);
    config.Opts->AddLongOption("deferred-ext-publication-id", "Optional deferred publication ext_publication_id")
        .Optional()
        .RequiredArgument("STRING")
        .StoreResult(&DeferredExtPublicationId_);

    AddTransform(config);
}

void TCommandExperimentalTopicWrite::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    ParseTopicName(config, 0);
    ParseMessagingFormats();
    ParseTransform();
    ParseCodec();

    if (Delimiter_.Defined() && MessagingFormat != EMessagingFormat::SingleMessage) {
        throw TMisuseException() << "Both mutually exclusive options \"delimiter\" and \"input-format\" were provided.";
    }
    if (DeferredIntPublicationId_.Defined() && *DeferredIntPublicationId_ == 0) {
        throw TMisuseException() << "--deferred-int-publication-id must be a positive integer";
    }
    if (DeferredExtPublicationId_.Defined() && !DeferredIntPublicationId_.Defined()) {
        throw TMisuseException() << "--deferred-ext-publication-id requires --deferred-int-publication-id";
    }
}

NTopic::TWriteSessionSettings TCommandExperimentalTopicWrite::PrepareWriteSessionSettings() {
    NTopic::TWriteSessionSettings settings;
    if (auto codec = GetCodec(); codec.Defined()) {
        settings.Codec(*codec);
    }
    settings.Path(TopicName);

    if (PartitionId_.Defined()) {
        settings.PartitionId(*PartitionId_);
    }
    if (!MessageGroupId_.Defined()) {
        const TString rnd = ToString(TInstant::Now().NanoSeconds());
        SHA_CTX ctx;
        SHA1_Init(&ctx);
        SHA1_Update(&ctx, rnd.data(), rnd.size());
        unsigned char sha1[SHA_DIGEST_LENGTH];
        SHA1_Final(sha1, &ctx);

        TString hex = HexEncode(TString(reinterpret_cast<const char*>(sha1), SHA_DIGEST_LENGTH));
        hex.to_lower();
        MessageGroupId_ = TString(hex.begin(), hex.begin() + 6);
    }

    settings.MessageGroupId(*MessageGroupId_);
    settings.ProducerId(*MessageGroupId_);
    return settings;
}

int TCommandExperimentalTopicWrite::Run(TConfig& config) {
    SetInterruptHandlers();

    auto driver = CreateDriver(config);
    auto writeSession = NTopic::TTopicClient(driver).CreateWriteSession(PrepareWriteSessionSettings());

    auto writerParams = TTopicWriterParams(
        MessagingFormat,
        Delimiter_,
        MessageSizeLimit_,
        Nothing(),
        Nothing(),
        Nothing(),
        GetTransform(),
        MessagesWaitTimeout_);

    if (DeferredIntPublicationId_.Defined()) {
        NTopic::TDeferredPublication deferred = DeferredExtPublicationId_.Defined()
            ? NTopic::TDeferredPublication(*DeferredIntPublicationId_, std::string(*DeferredExtPublicationId_))
            : NTopic::TDeferredPublication(*DeferredIntPublicationId_);
        writerParams.SetDeferredPublication(std::move(deferred));
    }

    TTopicWriter writer(writeSession, std::move(writerParams));
    if (int status = writer.Init(); status) {
        return status;
    }

    int status = 0;
    if (File_.Defined()) {
        TFileInput input(*File_);
        status = writer.Run(input);
    } else {
        status = writer.Run(Cin);
    }
    if (status) {
        return status;
    }
    if (!writer.Close()) {
        Cerr << "Failed to close session" << Endl;
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}

TCommandTopicDeferredPublication::TCommandTopicDeferredPublication()
    : TClientCommandTree("deferred-publication", {"publication", "dp"}, "Deferred topic publication operations")
{
    AddCommand(std::make_unique<TCommandTopicDeferredPublicationBegin>());
    AddCommand(std::make_unique<TCommandTopicDeferredPublicationPublish>());
    AddCommand(std::make_unique<TCommandTopicDeferredPublicationCancel>());
    AddCommand(std::make_unique<TCommandTopicDeferredPublicationList>());
    AddCommand(std::make_unique<TCommandTopicDeferredPublicationDescribe>());
}

TCommandTopicDeferredPublicationBegin::TCommandTopicDeferredPublicationBegin()
    : TYdbCommand("begin", {}, "Begin a deferred publication")
{
}

void TCommandTopicDeferredPublicationBegin::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->SetFreeArgsNum(0);
    config.Opts->AddLongOption("ext-publication-id", "Client-defined publication name (unique among active)")
        .Required()
        .RequiredArgument("STRING")
        .StoreResult(&ExtPublicationId_);
    config.Opts->AddLongOption("writer-identity", "Optional writer identity for List filter")
        .Optional()
        .RequiredArgument("STRING")
        .StoreResult(&WriterIdentity_);
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::Json,
    });
}

void TCommandTopicDeferredPublicationBegin::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandTopicDeferredPublicationBegin::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TDeferredPublishClient client(driver);
    NTopic::TBeginPublicationSettings settings;
    if (WriterIdentity_.Defined()) {
        settings.WriterIdentity(std::string(*WriterIdentity_));
    }

    auto result = client.BeginPublication(std::string(ExtPublicationId_), settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    if (OutputFormat == EDataFormat::Json) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["int_publication_id"] = result.GetIntPublicationId();
        json["ext_publication_id"] = ExtPublicationId_;
        WriteJsonLine(json);
    } else {
        Cout << result.GetIntPublicationId() << Endl;
    }
    return EXIT_SUCCESS;
}

TCommandTopicDeferredPublicationPublish::TCommandTopicDeferredPublicationPublish()
    : TYdbCommand(
          "publish",
          {},
          "Publish staged messages of a deferred publication. "
          "Run only after deferred write exits successfully; this command does not wait "
          "for in-flight writes from another process.")
{
}

void TCommandTopicDeferredPublicationPublish::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->SetFreeArgsNum(0);
    config.Opts->AddLongOption("int-publication-id", "Server-assigned publication id from begin")
        .Required()
        .RequiredArgument("UINT64")
        .StoreResult(&IntPublicationId_);
}

int TCommandTopicDeferredPublicationPublish::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TDeferredPublishClient client(driver);
    auto result = client.Publish(NTopic::TDeferredPublication(IntPublicationId_)).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    return EXIT_SUCCESS;
}

TCommandTopicDeferredPublicationCancel::TCommandTopicDeferredPublicationCancel()
    : TYdbCommand(
          "cancel",
          {},
          "Cancel a deferred publication and discard staged messages. "
          "Run only after deferred write exits successfully; this command does not wait "
          "for in-flight writes from another process.")
{
}

void TCommandTopicDeferredPublicationCancel::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->SetFreeArgsNum(0);
    config.Opts->AddLongOption("int-publication-id", "Server-assigned publication id from begin")
        .Required()
        .RequiredArgument("UINT64")
        .StoreResult(&IntPublicationId_);
}

int TCommandTopicDeferredPublicationCancel::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TDeferredPublishClient client(driver);
    auto result = client.CancelPublication(NTopic::TDeferredPublication(IntPublicationId_)).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    return EXIT_SUCCESS;
}

TCommandTopicDeferredPublicationList::TCommandTopicDeferredPublicationList()
    : TYdbCommand("list", {}, "List active deferred publications")
{
}

void TCommandTopicDeferredPublicationList::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->SetFreeArgsNum(0);
    config.Opts->AddLongOption("writer-identity", "Filter by writer identity from begin")
        .Optional()
        .RequiredArgument("STRING")
        .StoreResult(&WriterIdentity_);
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::Json,
    });
}

void TCommandTopicDeferredPublicationList::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandTopicDeferredPublicationList::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TDeferredPublishClient client(driver);
    NTopic::TListPublicationsSettings settings;
    if (WriterIdentity_.Defined()) {
        settings.WriterIdentity(std::string(*WriterIdentity_));
    }

    auto result = client.ListPublications(settings).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    if (OutputFormat == EDataFormat::Json) {
        NJson::TJsonValue json(NJson::JSON_ARRAY);
        for (const auto& publication : result.GetPublications()) {
            NJson::TJsonValue item(NJson::JSON_MAP);
            item["int_publication_id"] = publication.IntPublicationId;
            item["ext_publication_id"] = TString(publication.ExtPublicationId);
            if (publication.WriterIdentity) {
                item["writer_identity"] = TString(*publication.WriterIdentity);
            }
            json.AppendValue(std::move(item));
        }
        WriteJsonLine(json);
        return EXIT_SUCCESS;
    }

    TPrettyTable table({
        "int_publication_id",
        "ext_publication_id",
        "writer_identity",
    });
    for (const auto& publication : result.GetPublications()) {
        auto& row = table.AddRow();
        row.Column(0, publication.IntPublicationId);
        row.Column(1, TString(publication.ExtPublicationId));
        row.Column(2, publication.WriterIdentity ? TString(*publication.WriterIdentity) : TString("-"));
    }
    Cout << table;
    return EXIT_SUCCESS;
}

TCommandTopicDeferredPublicationDescribe::TCommandTopicDeferredPublicationDescribe()
    : TYdbCommand("describe", {}, "Describe an active deferred publication")
{
}

void TCommandTopicDeferredPublicationDescribe::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->SetFreeArgsNum(0);
    config.Opts->AddLongOption("int-publication-id", "Server-assigned publication id from begin")
        .Required()
        .RequiredArgument("UINT64")
        .StoreResult(&IntPublicationId_);
    AddOutputFormats(config, {
        EDataFormat::Pretty,
        EDataFormat::Json,
    });
}

void TCommandTopicDeferredPublicationDescribe::Parse(TConfig& config) {
    TYdbCommand::Parse(config);
    ParseOutputFormats();
}

int TCommandTopicDeferredPublicationDescribe::Run(TConfig& config) {
    auto driver = CreateDriver(config);
    TDeferredPublishClient client(driver);
    auto result = client.DescribePublication(NTopic::TDeferredPublication(IntPublicationId_)).GetValueSync();
    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);

    const auto& publication = result.GetPublication();
    if (OutputFormat == EDataFormat::Json) {
        NJson::TJsonValue json(NJson::JSON_MAP);
        json["int_publication_id"] = IntPublicationId_;
        json["ext_publication_id"] = TString(publication.ExtPublicationId);
        if (publication.WriterIdentity) {
            json["writer_identity"] = TString(*publication.WriterIdentity);
        }
        json["created_at"] = publication.CreatedAt.ToString();
        if (publication.CreatedBy) {
            json["created_by"] = TString(*publication.CreatedBy);
        }
        NJson::TJsonValue destinations(NJson::JSON_ARRAY);
        for (const auto& destination : publication.Destinations) {
            NJson::TJsonValue dest(NJson::JSON_MAP);
            dest["topic_path"] = TString(destination.TopicPath);
            NJson::TJsonValue partitionIds(NJson::JSON_ARRAY);
            for (const auto partitionId : destination.PartitionIds) {
                partitionIds.AppendValue(partitionId);
            }
            dest["partition_ids"] = std::move(partitionIds);
            destinations.AppendValue(std::move(dest));
        }
        json["destinations"] = std::move(destinations);
        WriteJsonLine(json);
        return EXIT_SUCCESS;
    }

    Cout << "int_publication_id: " << IntPublicationId_ << Endl;
    Cout << "ext_publication_id: " << publication.ExtPublicationId << Endl;
    if (publication.WriterIdentity) {
        Cout << "writer_identity: " << *publication.WriterIdentity << Endl;
    }
    Cout << "created_at: " << publication.CreatedAt << Endl;
    if (publication.CreatedBy) {
        Cout << "created_by: " << *publication.CreatedBy << Endl;
    }
    Cout << "destinations:" << Endl;
    if (publication.Destinations.empty()) {
        Cout << "  (none)" << Endl;
    } else {
        for (const auto& destination : publication.Destinations) {
            Cout << "  - topic: " << destination.TopicPath << Endl;
            Cout << "    partitions:";
            for (const auto partitionId : destination.PartitionIds) {
                Cout << " " << partitionId;
            }
            Cout << Endl;
        }
    }
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient

#include "ydb_proxy.h"

#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/ut_helpers/write_topic.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>

namespace NKikimr::NReplication {

Y_UNIT_TEST_SUITE(YdbProxy) {
    template <bool UseDatabase = true>
    class TEnv: public NTestHelpers::TEnv<UseDatabase> {
        using TBase = NTestHelpers::TEnv<UseDatabase>;

    public:
        using TBase::TBase;
        using TBase::Send;

        template <typename TEvResponse>
        auto Send(IEventBase* ev) {
            return TBase::template Send<TEvResponse>(this->GetYdbProxy(), ev);
        }
    };

    Y_UNIT_TEST(MakeDirectory) {
        TEnv env;
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(RemoveDirectory) {
        TEnv env;
        // make
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvRemoveDirectoryResponse>(
                new TEvYdbProxy::TEvRemoveDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvRemoveDirectoryResponse>(
                new TEvYdbProxy::TEvRemoveDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(DescribePath) {
        TEnv env;
        // describe root
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribePathResponse>(
                new TEvYdbProxy::TEvDescribePathRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& entry = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::Directory);
        }
        // make dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // describe dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribePathResponse>(
                new TEvYdbProxy::TEvDescribePathRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& entry = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(entry.Name, "dir");
            UNIT_ASSERT_VALUES_EQUAL(entry.Type, NYdb::NScheme::ESchemeEntryType::Directory);
        }
    }

    Y_UNIT_TEST(ListDirectory) {
        TEnv env;
        // describe empty root
        {
            auto ev = env.Send<TEvYdbProxy::TEvListDirectoryResponse>(
                new TEvYdbProxy::TEvListDirectoryRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& self = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(self.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(self.Type, NYdb::NScheme::ESchemeEntryType::Directory);

            const auto& children = ev->Get()->Result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(children[0].Name, ".sys");
            UNIT_ASSERT_VALUES_EQUAL(children[0].Type, NYdb::NScheme::ESchemeEntryType::Directory);
        }
        // make dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // describe non-empty root
        {
            auto ev = env.Send<TEvYdbProxy::TEvListDirectoryResponse>(
                new TEvYdbProxy::TEvListDirectoryRequest("/Root", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& self = ev->Get()->Result.GetEntry();
            UNIT_ASSERT_VALUES_EQUAL(self.Name, "Root");
            UNIT_ASSERT_VALUES_EQUAL(self.Type, NYdb::NScheme::ESchemeEntryType::Directory);

            const auto& children = ev->Get()->Result.GetChildren();
            UNIT_ASSERT_VALUES_EQUAL(children.size(), 2);
        }
    }

    Y_UNIT_TEST(StaticCreds) {
        TEnv<true> env("user1", "password1");
        // make dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(OAuthToken) {
        TEnv<true> env("user@builtin");
        // make dir
        {
            auto ev = env.Send<TEvYdbProxy::TEvMakeDirectoryResponse>(
                new TEvYdbProxy::TEvMakeDirectoryRequest("/Root/dir", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(CreateTable) {
        TEnv<false> env;
        // invalid key
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Float) // cannot be key
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // ok, created
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok, exists
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(DropTable) {
        TEnv<false> env;
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTableResponse>(
                new TEvYdbProxy::TEvDropTableRequest("/Root/table", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTableResponse>(
                new TEvYdbProxy::TEvDropTableRequest("/Root/table", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(AlterTable) {
        TEnv<false> env;
        // fail
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropColumns("extra");

            auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .AddNullableColumn("extra", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropColumns("extra");

            auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // invalid column
        {
            auto settings = NYdb::NTable::TAlterTableSettings()
                .AppendDropColumns("extra"); // not exist

            auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(CopyTable) {
        TEnv<false> env;
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvCopyTableResponse>(
                new TEvYdbProxy::TEvCopyTableRequest("/Root/table", "/Root/copy", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvCopyTableResponse>(
                new TEvYdbProxy::TEvCopyTableRequest("/Root/table", "/Root/copy", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(CopyTables) {
        TEnv<false> env;

        TVector<NYdb::NTable::TCopyItem> items{
            {"/Root/table1", "/Root/copy1"},
            {"/Root/table2", "/Root/copy2"},
        };

        // create
        for (const auto& item : items) {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest(item.SourcePath(), std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // copy
        {

            auto ev = env.Send<TEvYdbProxy::TEvCopyTablesResponse>(
                new TEvYdbProxy::TEvCopyTablesRequest(std::move(items), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(DescribeTable) {
        TEnv<false> env;
        // create
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // describe
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeTableResponse>(
                new TEvYdbProxy::TEvDescribeTableRequest("/Root/table", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& schema = ev->Get()->Result.GetTableDescription();
            // same pk
            UNIT_ASSERT_EQUAL(schema.GetPrimaryKeyColumns().size(), 1);
            UNIT_ASSERT_EQUAL(schema.GetPrimaryKeyColumns().at(0), "key");
            // same columns
            UNIT_ASSERT_EQUAL(schema.GetColumns().size(), 2);
            UNIT_ASSERT_EQUAL(schema.GetColumns().at(0).Name, "key");
            UNIT_ASSERT_EQUAL(schema.GetColumns().at(1).Name, "value");
        }
    }

    Y_UNIT_TEST(CreateCdcStream) {
        TEnv<false> env;
        // create table
        {
            auto schema = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("value", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("key")
                .Build();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTableResponse>(
                new TEvYdbProxy::TEvCreateTableRequest("/Root/table", std::move(schema), {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }

        const auto feed = NYdb::NTable::TChangefeedDescription("updates",
            NYdb::NTable::EChangefeedMode::Updates, NYdb::NTable::EChangefeedFormat::Json
        );

        // two attempts: create, check, retry, check
        for (int i = 1; i <= 2; ++i) {
            // create cdc stream
            {
                auto settings = NYdb::NTable::TAlterTableSettings()
                    .AppendAddChangefeeds(feed);

                auto ev = env.Send<TEvYdbProxy::TEvAlterTableResponse>(
                    new TEvYdbProxy::TEvAlterTableRequest("/Root/table", settings));
                UNIT_ASSERT(ev);
                UNIT_ASSERT(ev->Get()->Result.IsSuccess());
            }
            // describe
            {
                auto ev = env.Send<TEvYdbProxy::TEvDescribeTableResponse>(
                    new TEvYdbProxy::TEvDescribeTableRequest("/Root/table", {}));
                UNIT_ASSERT(ev);
                UNIT_ASSERT(ev->Get()->Result.IsSuccess());

                const auto& schema = ev->Get()->Result.GetTableDescription();
                UNIT_ASSERT_EQUAL(schema.GetChangefeedDescriptions().size(), 1);
                UNIT_ASSERT_EQUAL(schema.GetChangefeedDescriptions().at(0), feed);
            }
        }
    }

    Y_UNIT_TEST(CreateTopic) {
        TEnv env;
        // invalid retention period
        {
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .RetentionPeriod(TDuration::Days(365));

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
    }

    Y_UNIT_TEST(AlterTopic) {
        TEnv env;
        // fail
        {
            auto settings = NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionPeriod(TDuration::Days(2));

            auto ev = env.Send<TEvYdbProxy::TEvAlterTopicResponse>(
                new TEvYdbProxy::TEvAlterTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto settings = NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionPeriod(TDuration::Days(2));

            auto ev = env.Send<TEvYdbProxy::TEvAlterTopicResponse>(
                new TEvYdbProxy::TEvAlterTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // invalid retention period
        {
            auto settings = NYdb::NTopic::TAlterTopicSettings()
                .SetRetentionPeriod(TDuration::Days(365));

            auto ev = env.Send<TEvYdbProxy::TEvAlterTopicResponse>(
                new TEvYdbProxy::TEvAlterTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(DropTopic) {
        TEnv env;
        // create
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTopicResponse>(
                new TEvYdbProxy::TEvDropTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDropTopicResponse>(
                new TEvYdbProxy::TEvDropTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    Y_UNIT_TEST(DescribeTopic) {
        TEnv env;
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeTopicResponse>(
                new TEvYdbProxy::TEvDescribeTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeTopicResponse>(
                new TEvYdbProxy::TEvDescribeTopicRequest("/Root/topic", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());

            const auto& schema = ev->Get()->Result.GetTopicDescription();
            UNIT_ASSERT_VALUES_EQUAL(schema.GetTotalPartitionsCount(), 1);
            UNIT_ASSERT_VALUES_EQUAL(schema.GetRetentionPeriod(), TDuration::Days(1));
        }
    }

    Y_UNIT_TEST(DescribeConsumer) {
        TEnv env;
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeConsumerResponse>(
                new TEvYdbProxy::TEvDescribeConsumerRequest("/Root/topic", "consumer", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
        // create
        {
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .BeginAddConsumer()
                    .ConsumerName("consumer")
                .EndAddConsumer();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // ok
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeConsumerResponse>(
                new TEvYdbProxy::TEvDescribeConsumerRequest("/Root/topic", "consumer", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }
        // fail
        {
            auto ev = env.Send<TEvYdbProxy::TEvDescribeConsumerResponse>(
                new TEvYdbProxy::TEvDescribeConsumerRequest("/Root/topic", "consumer2", {}));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(!ev->Get()->Result.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
        }
    }

    template <typename Env>
    TActorId CreateTopicReader(Env& env, const TString& topicPath) {
        auto settings = TEvYdbProxy::TTopicReaderSettings()
            .ConsumerName("consumer")
            .AppendTopics(NYdb::NTopic::TTopicReadSettings(topicPath));

        auto ev = env.template Send<TEvYdbProxy::TEvCreateTopicReaderResponse>(
            new TEvYdbProxy::TEvCreateTopicReaderRequest(settings));
        UNIT_ASSERT(ev);
        UNIT_ASSERT(ev->Get()->Result);

        return ev->Get()->Result;
    }

    template <typename Env>
    TEvYdbProxy::TReadTopicResult ReadTopicData(Env& env, TActorId& reader, const TString& topicPath) {
        do {
            env.SendAsync(reader, new TEvYdbProxy::TEvReadTopicRequest());

            try {
                TAutoPtr<IEventHandle> ev;
                env.GetRuntime().template GrabEdgeEventsRethrow<TEvYdbProxy::TEvReadTopicResponse, TEvYdbProxy::TEvTopicReaderGone>(ev);
                UNIT_ASSERT_VALUES_EQUAL(ev->Sender, reader);

                switch (ev->GetTypeRewrite()) {
                case TEvYdbProxy::EvReadTopicResponse:
                    return ev->Get<TEvYdbProxy::TEvReadTopicResponse>()->Result;
                case TEvYdbProxy::EvTopicReaderGone:
                    ythrow yexception();
                }
            } catch (yexception&) {
                reader = CreateTopicReader(env, topicPath);
            }
        } while (true);
    }

    Y_UNIT_TEST(ReadTopic) {
        TEnv env;

        // create topic
        {
            auto settings = NYdb::NTopic::TCreateTopicSettings()
                .BeginAddConsumer()
                    .ConsumerName("consumer")
                .EndAddConsumer();

            auto ev = env.Send<TEvYdbProxy::TEvCreateTopicResponse>(
                new TEvYdbProxy::TEvCreateTopicRequest("/Root/topic", settings));
            UNIT_ASSERT(ev);
            UNIT_ASSERT(ev->Get()->Result.IsSuccess());
        }

        TActorId reader = CreateTopicReader(env, "/Root/topic");

        UNIT_ASSERT(NTestHelpers::WriteTopic(env, "/Root/topic", "message-0"));
        {
            auto data = ReadTopicData(env, reader, "/Root/topic");
            UNIT_ASSERT_VALUES_EQUAL(data.Messages.size(), 1);

            const auto& msg = data.Messages.at(0);
            UNIT_ASSERT_VALUES_EQUAL(msg.GetOffset(), 0);
            UNIT_ASSERT_VALUES_EQUAL(msg.GetData(), "message-0");
        }

        // wait next event
        env.SendAsync(reader, new TEvYdbProxy::TEvReadTopicRequest());

        TActorId newReader;
        do {
            newReader = CreateTopicReader(env, "/Root/topic");
            // wait next event
            env.SendAsync(newReader, new TEvYdbProxy::TEvReadTopicRequest());

            // wait event from previous session
            try {
                auto ev = env.GetRuntime().GrabEdgeEventRethrow<TEvYdbProxy::TEvTopicReaderGone>(env.GetSender());
                if (ev->Sender == reader) {
                    break;
                } else if (ev->Sender == newReader) {
                    continue;
                } else {
                    UNIT_ASSERT("Unexpected reader has gone");
                }
            } catch (yexception&) {
                // bad luck, previous session was not closed, close it manually
                env.SendAsync(reader, new TEvents::TEvPoison());
                break;
            }
        } while (true);

        UNIT_ASSERT(NTestHelpers::WriteTopic(env, "/Root/topic", "message-1"));
        {
            auto data = ReadTopicData(env, newReader, "/Root/topic");
            UNIT_ASSERT(data.Messages.size() >= 1);

            for (int i = data.Messages.size() - 1; i >= 0; --i) {
                const auto offset = i + int(data.Messages.size() == 1);
                const auto& msg = data.Messages.at(i);
                UNIT_ASSERT_VALUES_EQUAL(msg.GetOffset(), offset);
                UNIT_ASSERT_VALUES_EQUAL(msg.GetData(), Sprintf("message-%i", offset));
            }
        }
    }

    Y_UNIT_TEST(ReadNonExistentTopic) {
        TEnv env;

        auto reader = CreateTopicReader(env, "/Root/topic");
        auto ev = env.Send<TEvYdbProxy::TEvTopicReaderGone>(reader, new TEvYdbProxy::TEvReadTopicRequest());

        UNIT_ASSERT(ev);
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Result.GetStatus(), NYdb::EStatus::SCHEME_ERROR);
    }

} // YdbProxyTests

}

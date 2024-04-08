#include "run_ydb.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/join.h>
#include <util/string/strip.h>
#include <util/system/env.h>
#include <util/system/shellcommand.h>

Y_UNIT_TEST_SUITE(YdbTopic) {

class TSupportedCodecsFixture : public NUnitTest::TBaseFixture {
protected:
    using TCodecList = THashSet<TString>;

    void TestTopicCreate(const TString& topicName,
                         const TCodecList& supportedCodecs,
                         const TCodecList& expectedSupportedCodecs);
    void TestTopicAlter(const TString& topicName,
                        const TCodecList& supportedCodecs,
                        const TCodecList& expectedSupportedCodecs);
    void TestTopicConsumerAdd(const TString& topicName,
                              const TString& consumerName,
                              const TCodecList& supportedCodecs,
                              const TCodecList& expectedSupportedCodecs);

    TString YdbSchemeDescribe(const TString& topicName);

    void YdbTopicCreate(const TString& topicName,
                        const TCodecList& supportedCodecs = {});
    void YdbTopicAlter(const TString& topicName,
                       const TCodecList& supportedCodecs = {});
    void YdbTopicConsumerAdd(const TString& topicName,
                             const TString& consumerName,
                             const TCodecList& supportedCodecs = {});

    TString GetTopicName(unsigned suffix = 0) const;
    TString GetConsumerName(unsigned suffix = 0) const;

    static TString GetObjectName(const TString& prefix, const TString& base, unsigned suffix);
    static TString ExecYdb(const TList<TString>& cmd, bool checkExitCode = true);
    static TCodecList GetTopicSupportedCodecs(const TString& description);
    static TCodecList GetTopicConsumerSupportedCodecs(const TString& description,
                                                      const TString& consumerName);
};

Y_UNIT_TEST_F(SupportedCodecs_TopicCreate_DefaultValue, TSupportedCodecsFixture)
{
    TestTopicCreate(GetTopicName(), {}, {"RAW"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicCreate_UserValue, TSupportedCodecsFixture)
{
    TestTopicCreate(GetTopicName(), {"GZIP", "ZSTD"}, {"GZIP", "ZSTD"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicAlter, TSupportedCodecsFixture)
{
    TString topicName = GetTopicName();

    YdbTopicCreate(topicName);

    TestTopicAlter(topicName, {}, {"RAW"});
    TestTopicAlter(topicName, {"GZIP", "ZSTD"}, {"GZIP", "ZSTD"});
    TestTopicAlter(topicName, {}, {"GZIP", "ZSTD"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicConsumerAdd_DefaultValue, TSupportedCodecsFixture)
{
    TString topicName = GetTopicName();
    TString consumerName = GetConsumerName();

    YdbTopicCreate(topicName);

    TestTopicConsumerAdd(topicName, consumerName, {}, {"RAW"});
}

Y_UNIT_TEST_F(SupportedCodecs_TopicConsumerAdd_UserValue, TSupportedCodecsFixture)
{
    TString topicName = GetTopicName();
    TString consumerName[2] = { GetConsumerName(0), GetConsumerName(1) };

    YdbTopicCreate(topicName, {"GZIP", "ZSTD"});

    TestTopicConsumerAdd(topicName, consumerName[0], {"GZIP", "ZSTD"}, {"GZIP", "ZSTD"});
    TestTopicConsumerAdd(topicName, consumerName[1], {"GZIP", "ZSTD", "RAW"}, {"GZIP", "ZSTD", "RAW"});
}

void TSupportedCodecsFixture::TestTopicCreate(const TString& topicName,
                                              const TCodecList& supportedCodecs,
                                              const TCodecList& expectedSupportedCodecs)
{
    YdbTopicCreate(topicName, supportedCodecs);
    TString description = YdbSchemeDescribe(topicName);
    THashSet<TString> actualSupportedCodecs = GetTopicSupportedCodecs(description);
    UNIT_ASSERT_VALUES_EQUAL(actualSupportedCodecs, expectedSupportedCodecs);
}

void TSupportedCodecsFixture::TestTopicAlter(const TString& topicName,
                                             const TCodecList& supportedCodecs,
                                             const TCodecList& expectedSupportedCodecs)
{
    YdbTopicAlter(topicName, supportedCodecs);
    TString description = YdbSchemeDescribe(topicName);
    THashSet<TString> actualSupportedCodecs = GetTopicSupportedCodecs(description);
    UNIT_ASSERT_VALUES_EQUAL(actualSupportedCodecs, expectedSupportedCodecs);
}

void TSupportedCodecsFixture::TestTopicConsumerAdd(const TString& topicName,
                                                   const TString& consumerName,
                                                   const TCodecList& supportedCodecs,
                                                   const TCodecList& expectedSupportedCodecs)
{
    TString description = YdbSchemeDescribe(topicName);
    YdbTopicConsumerAdd(topicName, consumerName, supportedCodecs);
    description = YdbSchemeDescribe(topicName);
    THashSet<TString> actualSupportedCodecs = GetTopicConsumerSupportedCodecs(description,
                                                                              consumerName);
    UNIT_ASSERT_VALUES_EQUAL(actualSupportedCodecs, expectedSupportedCodecs);
}

TString TSupportedCodecsFixture::YdbSchemeDescribe(const TString& topicName)
{
    TList<TString> cmd;
    cmd.push_back("scheme");
    cmd.push_back("describe");
    cmd.push_back(topicName);

    return ExecYdb(cmd);
}

void TSupportedCodecsFixture::YdbTopicCreate(const TString& topicName,
                                             const TCodecList& supportedCodecs)
{
    TList<TString> cmd;
    cmd.push_back("topic");
    cmd.push_back("create");
    if (!supportedCodecs.empty()) {
        cmd.push_back("--supported-codecs");
        cmd.push_back(JoinRange(",", supportedCodecs.begin(), supportedCodecs.end()));
    }
    cmd.push_back(topicName);

    ExecYdb(cmd);
}

void TSupportedCodecsFixture::YdbTopicAlter(const TString& topicName,
                                            const TCodecList& supportedCodecs)
{
    TList<TString> cmd;
    cmd.push_back("topic");
    cmd.push_back("alter");
    if (!supportedCodecs.empty()) {
        cmd.push_back("--supported-codecs");
        cmd.push_back(JoinRange(",", supportedCodecs.begin(), supportedCodecs.end()));
    }
    cmd.push_back("--partitions-count");
    cmd.push_back("2");
    cmd.push_back(topicName);

    ExecYdb(cmd);
}

void TSupportedCodecsFixture::YdbTopicConsumerAdd(const TString& topicName,
                                                  const TString& consumerName,
                                                  const TCodecList& supportedCodecs)
{
    TList<TString> cmd;
    cmd.push_back("topic");
    cmd.push_back("consumer");
    cmd.push_back("add");
    cmd.push_back("--consumer");
    cmd.push_back(consumerName);
    if (!supportedCodecs.empty()) {
        cmd.push_back("--supported-codecs");
        cmd.push_back(JoinRange(",", supportedCodecs.begin(), supportedCodecs.end()));
    }
    cmd.push_back(topicName);

    ExecYdb(cmd);
}

TString TSupportedCodecsFixture::GetTopicName(unsigned suffix) const
{
    return GetObjectName("Topic", Name_, suffix);
}

TString TSupportedCodecsFixture::GetConsumerName(unsigned suffix) const
{
    return GetObjectName("Consumer", Name_, suffix);
}

TString TSupportedCodecsFixture::GetObjectName(const TString& prefix, const TString& base, unsigned suffix)
{
    TStringStream name;
    name << prefix;
    name << "_";
    name << base;
    name << "_";
    name << suffix;
    return name.Str();
}

TString TSupportedCodecsFixture::ExecYdb(const TList<TString>& args, bool checkExitCode)
{
    //
    // ydb -e grpc://${YDB_ENDPOINT} -d /${YDB_DATABASE} ${args}
    //
    return RunYdb({}, args, checkExitCode);
}

auto TSupportedCodecsFixture::GetTopicSupportedCodecs(const TString& description) -> TCodecList
{
    //<topic> topic
    //
    //RetentionPeriod: 18 hours
    //PartitionsCount: 1
    //PartitionWriteSpeed: 2048 KB
    //MeteringMode: Unspecified
    //SupportedCodecs: RAW, GZIP, LZOP
    //
    //Consumers: 
    //┌───────────────────────────┬───────────────────────┬───────────────────────────────┬───────────┐
    //│ ConsumerName              │ SupportedCodecs       │ ReadFrom                      │ Important │
    //├───────────────────────────┼───────────────────────┼───────────────────────────────┼───────────┤
    //│ consumer                  │ RAW, GZIP, LZOP, ZSTD │ Tue, 02 Apr 2024 17:11:47 MSK │ No        │
    //└───────────────────────────┴───────────────────────┴───────────────────────────────┴───────────┘

    TVector<TString> lines;
    Split(description, "\n", lines);

    for (auto& line : lines) {
        if (line.StartsWith("SupportedCodecs:")) {
            TVector<TString> fields;
            Split(line, ":", fields);

            THashSet<TString> result;

            TVector<TString> codecs;
            Split(fields[1], ",", codecs);
            for (auto& codec : codecs) {
                result.insert(Strip(codec));
            }

            return result;
        }
    }

    return {};
}

auto TSupportedCodecsFixture::GetTopicConsumerSupportedCodecs(const TString& description,
                                                              const TString& consumerName) -> TCodecList
{
    //<topic> topic
    //
    //RetentionPeriod: 18 hours
    //PartitionsCount: 1
    //PartitionWriteSpeed: 2048 KB
    //MeteringMode: Unspecified
    //SupportedCodecs: RAW, GZIP, LZOP
    //
    //Consumers: 
    //┌───────────────────────────┬───────────────────────┬───────────────────────────────┬───────────┐
    //│ ConsumerName              │ SupportedCodecs       │ ReadFrom                      │ Important │
    //├───────────────────────────┼───────────────────────┼───────────────────────────────┼───────────┤
    //│ consumer                  │ RAW, GZIP, LZOP, ZSTD │ Tue, 02 Apr 2024 17:11:47 MSK │ No        │
    //└───────────────────────────┴───────────────────────┴───────────────────────────────┴───────────┘

    TVector<TString> lines;
    Split(description, "\n", lines);

    bool beforeConsumers = true;

    for (auto& line : lines) {
        if (beforeConsumers) {
            if (line.StartsWith("Consumers:")) {
                beforeConsumers = false;
            }
        } else {
            if (line.StartsWith("│ " + consumerName)) {
                TVector<TString> fields;
                Split(line, "│", fields);

                THashSet<TString> result;

                TVector<TString> codecs;
                Split(fields[1], ",", codecs);
                for (auto& codec : codecs) {
                    result.insert(Strip(codec));
                }

                return result;
            }
        }
    }

    return {};
}

}

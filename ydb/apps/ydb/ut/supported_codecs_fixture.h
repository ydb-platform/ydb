#pragma once

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/list.h>
#include <util/generic/string.h>

namespace NYdbCliTests {

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

}

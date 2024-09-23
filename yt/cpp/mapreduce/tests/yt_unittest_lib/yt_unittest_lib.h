#pragma once

#include <yt/cpp/mapreduce/interface/logging/logger.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <yt/cpp/mapreduce/interface/config.h>

#include <library/cpp/yson/node/node_io.h>

#include <util/generic/bt_exception.h>

#include <util/datetime/base.h>

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::TNode>(IOutputStream& s, const NYT::TNode& node);

template <>
void Out<TGUID>(IOutputStream& s, const TGUID& guid);

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NTesting {

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateTestClient(TString proxy = "", TCreateClientOptions options = {});

// Create map node by unique path in Cypress and return that path.
TYPath CreateTestDirectory(const IClientBasePtr& client);

TString GenerateRandomData(size_t size, ui64 seed = 42);

TVector<TNode> ReadTable(const IClientBasePtr& client, const TString& tablePath);
void WriteTable(const IClientBasePtr& client, const TString& tablePath, const std::vector<TNode>& rowList);

template <class TMessage>
TVector<TMessage> ReadProtoTable(const IClientBasePtr& client, const TString& tablePath);

template <class TMessage>
void WriteProtoTable(const IClientBasePtr& client, const TString& tablePath, const std::vector<TMessage>& rowList);

////////////////////////////////////////////////////////////////////////////////

// TODO: should be removed, usages should be replaced with TConfigSaverGuard
class TZeroWaitLockPollIntervalGuard
{
public:
    TZeroWaitLockPollIntervalGuard();

    ~TZeroWaitLockPollIntervalGuard();

private:
    TDuration OldWaitLockPollInterval_;
};

////////////////////////////////////////////////////////////////////////////////

class TConfigSaverGuard
{
public:
    TConfigSaverGuard();
    ~TConfigSaverGuard();

private:
    TConfig Config_;
};

////////////////////////////////////////////////////////////////////////////////

class TDebugMetricDiff
{
public:
    TDebugMetricDiff(TString name);
    ui64 GetTotal() const;

private:
    TString Name_;
    ui64 InitialValue_;
};

////////////////////////////////////////////////////////////////////////////////

struct TOwningYaMRRow
{
    TString Key;
    TString SubKey;
    TString Value;

    TOwningYaMRRow(const TYaMRRow& row = {});
    TOwningYaMRRow(TString key, TString subKey, TString value);

    operator TYaMRRow() const;
};

bool operator == (const TOwningYaMRRow& row1, const TOwningYaMRRow& row2);

////////////////////////////////////////////////////////////////////////////////

class TTestFixture
{
public:
    explicit TTestFixture(const TCreateClientOptions& options = {});
    ~TTestFixture();

    // Return precreated client.
    IClientPtr GetClient() const;

    // Return newly created client. Useful for cases:
    //  - when we want to have multiple clients objects;
    //  - when we want to control to control destruction of client object;
    IClientPtr CreateClient(const TCreateClientOptions& options = {}) const;

    IClientPtr CreateClientForUser(const TString& user, TCreateClientOptions options = {});

    TYPath GetWorkingDir() const;

    static TString GetYtProxy();

private:
    TConfigSaverGuard ConfigGuard_;
    IClientPtr Client_;
    TYPath WorkingDir_;
};

////////////////////////////////////////////////////////////////////////////////

class TTabletFixture
    : public TTestFixture
{
public:
    TTabletFixture();

private:
    void WaitForTabletCell();
};

////////////////////////////////////////////////////////////////////////////////

// Compares only columns and only "name" and "type" fields of columns.
bool AreSchemasEqual(const TTableSchema& lhs, const TTableSchema& rhs);

class TWaitFailedException
    : public TWithBackTrace<yexception>
{ };

void WaitForPredicate(const std::function<bool()>& predicate, TDuration timeout = TDuration::Seconds(60));

////////////////////////////////////////////////////////////////////////////////

// Redirects all the LOG_* calls with the corresponding level to `stream`.
// Moreover, the LOG_* calls are delegated to `oldLogger`.
class TStreamTeeLogger
    : public ILogger
{
public:
    TStreamTeeLogger(ELevel cutLevel, IOutputStream* stream, ILoggerPtr oldLogger);
    void Log(ELevel level, const ::TSourceLocation& sourceLocation, const char* format, va_list args) override;

private:
    ILoggerPtr OldLogger_;
    IOutputStream* Stream_;
    ELevel Level_;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString ToYson(const T& x)
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(x, &builder);
    return NodeToYsonString(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTesting

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NYT::NTesting::TOwningYaMRRow>(IOutputStream& out, const NYT::NTesting::TOwningYaMRRow& row);

////////////////////////////////////////////////////////////////////////////////

// for UNITTEST()
#define ASSERT_SERIALIZABLES_EQUAL(a, b) \
    UNIT_ASSERT_EQUAL_C(a, b, NYT::NTesting::ToYson(a) << " != " << NYT::NTesting::ToYson(b))

#define ASSERT_SERIALIZABLES_UNEQUAL(a, b) \
    UNIT_ASSERT_UNEQUAL_C(a, b, NYT::NTesting::ToYson(a) << " == " << NYT::NTesting::ToYson(b))

// for GTEST()
#define ASSERT_SERIALIZABLES_EQ(a, b) \
    ASSERT_EQ(a, b) << NYT::NTesting::ToYson(a) << " != " << NYT::NTesting::ToYson(b)

#define ASSERT_SERIALIZABLES_NE(a, b) \
    ASSERT_NE(a, b) << NYT::NTesting::ToYson(a) << " == " << NYT::NTesting::ToYson(b)

#define YT_UNITTEST_LIB_H_
#include "yt_unittest_lib-inl.h"
#undef YT_UNITTEST_LIB_H_


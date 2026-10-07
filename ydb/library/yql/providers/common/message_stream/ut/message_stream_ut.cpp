#include <ydb/library/yql/providers/common/message_stream/provider.h>
#include <ydb/library/yql/providers/common/message_stream/partition.h>
#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <util/generic/yexception.h>
namespace NFq::NMessageStream {
using namespace NYql;
TEST(TMessageStreamCommon, SnapshotCompletion) {
    TPartitionProgress progress;
    EXPECT_FALSE(progress.IsFinishedInTableMode());
    progress.EndOffset = 0;
    EXPECT_TRUE(progress.IsFinishedInTableMode());
    progress.EndOffset = 43;
    EXPECT_FALSE(progress.IsFinishedInTableMode());
    progress.Offset = 42;
    EXPECT_FALSE(progress.IsFinishedInTableMode());
    progress.Offset = 43;
    EXPECT_TRUE(progress.IsFinishedInTableMode());
    progress.Offset = 100;
    EXPECT_TRUE(progress.IsFinishedInTableMode());
}
TEST(TMessageStreamCommon, WriteTimeCompletion) {
    TPartitionProgress progress;
    progress.EndWriteTime = TInstant::Seconds(10);
    progress.LastMessageWriteTime = TInstant::Seconds(9);
    EXPECT_FALSE(progress.IsFinishedInTableMode());
    progress.LastMessageWriteTime = TInstant::Seconds(10);
    EXPECT_TRUE(progress.IsFinishedInTableMode());
}
TEST(TMessageStreamCommon, CompletionPreservesOriginalError) {
    auto promise = NThreading::NewPromise<int>();
    auto done = CompletionFuture(promise.GetFuture());
    EXPECT_FALSE(done.HasValue());
    try { ythrow yexception() << "metadata failed"; }
    catch (...) { promise.SetException(std::current_exception()); }
    EXPECT_NO_THROW(done.GetValueSync());
    try {
        promise.GetFuture().GetValueSync();
        FAIL() << "Original future lost its exception";
    } catch (const yexception& error) {
        EXPECT_TRUE(TString(error.what()).Contains("metadata failed"));
    }
}
TEST(TMessageStreamCommon, NoneAuthHasNonemptyRepresentation) {
    const auto token = ComposeAuthToken({{"authMethod", "NONE"}});
    EXPECT_FALSE(token.empty());
    EXPECT_TRUE(CreateStructuredTokenParser(token).IsNoAuth());
}
TEST(TMessageStreamCommon, AuthMethodsPreserveSecretsAndReferences) {
    EXPECT_EQ(ComposeAuthToken({{"authMethod", "TOKEN"}, {"token", "secret"}, {"tokenReference", "ref"}}),
        ComposeStructuredTokenJsonForTokenAuthWithSecret("ref", "secret"));
    EXPECT_EQ(ComposeAuthToken({{"authMethod", "BASIC"}, {"login", "user"}, {"password", "secret"}, {"passwordReference", "ref"}}),
        ComposeStructuredTokenJsonForBasicAuthWithSecret("user", "ref", "secret"));
    EXPECT_EQ(ComposeAuthToken({{"authMethod", "IAM"}, {"iamServiceAccountId", "account"}, {"iamResourceId", "resource"}}),
        ComposeStructuredTokenJsonForIamAuth("account", "resource"));
    EXPECT_EQ(ComposeAuthToken({{"transient_token", "transient"}}), ComposeStructuredTokenJsonForTransientTokenAuth("transient"));
    EXPECT_EQ(ComposeAuthToken({}, "fallback", "account", "signature"),
        ComposeStructuredTokenJsonForServiceAccount("account", "signature", "fallback"));
}
}

namespace NFq::NMessageStream {
TEST(TMessageStreamCommon, RawSchemaPreservesDataStringContract) {
    TExprContext ctx;
    const auto* row = MakeRawRowType(ctx);
    ASSERT_EQ(row->GetItems().size(), 1u);
    const auto* item = row->GetItems().front();
    EXPECT_EQ(item->GetName(), "Data");
    ASSERT_EQ(item->GetItemType()->GetKind(), ETypeAnnotationKind::Data);
    EXPECT_EQ(item->GetItemType()->Cast<TDataExprType>()->GetSlot(), NUdf::EDataSlot::String);
}
TEST(TMessageStreamCommon, CompletionWaitsWithoutConsumingValue) {
    auto promise = NThreading::NewPromise<int>();
    const auto ready = CompletionFuture(promise.GetFuture());
    EXPECT_FALSE(ready.HasValue());
    promise.SetValue(42);
    EXPECT_NO_THROW(ready.GetValueSync());
    EXPECT_EQ(promise.GetFuture().GetValueSync(), 42);
    EXPECT_NO_THROW(CompletionFuture(promise.GetFuture()).GetValueSync());
}
}

#include <yt/yt/client/api/not_implemented_client.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NApi {
namespace {

using namespace NFlow;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TRecordingFlowClient
    : public TNotImplementedClient
{
public:
    std::string Command;
    NYPath::TYPath PipelinePath;
    TYsonString Argument;
    TFlowExecuteOptions Options;
    TYsonString Response = TYsonString(TStringBuf("{version=12;}"));

    TFuture<TFlowExecuteResult> FlowExecute(
        const NYPath::TYPath& pipelinePath,
        const std::string& command,
        const TYsonString& argument,
        const TFlowExecuteOptions& options) override
    {
        PipelinePath = pipelinePath;
        Command = command;
        Argument = argument;
        Options = options;
        return MakeFuture(TFlowExecuteResult{.Result = Response});
    }
};

TEST(TFlowClientTest, TypedDynamicSpecPatches)
{
    auto client = New<TRecordingFlowClient>();
    client->Response = TYsonString(TStringBuf(R"({
        version=11;
        patches=[
            {patch_id=7;changes=[{path="/a";value=3;}];comment="raise limit";timestamp="2026-10-03T12:00:00Z";};
            {patch_id=8;changes=[{path="/a";value=4;};{path="/b";value={x=5;};}];timestamp="2026-10-03T12:01:00Z";};
        ];
    })"));
    TGetPipelineDynamicSpecPatchesOptions options;
    options.Timeout = TDuration::Seconds(17);
    options.Patch = TVersion(7);
    auto result = WaitFor(client->GetPipelineDynamicSpecPatches("//pipeline", options))
        .ValueOrThrow();

    EXPECT_EQ(client->Command, "get-pipeline-dynamic-spec-patches");
    EXPECT_EQ(client->PipelinePath, "//pipeline");
    EXPECT_EQ(client->Options.Timeout, options.Timeout);
    EXPECT_EQ(ConvertTo<IMapNodePtr>(client->Argument)->GetChildValueOrThrow<TVersion>("patch"), TVersion(7));
    EXPECT_EQ(result.Version, TVersion(11));
    ASSERT_EQ(result.Patches.size(), 2u);
    EXPECT_EQ(result.Patches[0].PatchId, TVersion(7));
    EXPECT_EQ(result.Patches[0].Comment, "raise limit");
    ASSERT_EQ(result.Patches[0].Changes.size(), 1u);
    EXPECT_EQ(result.Patches[0].Changes[0].Path, "/a");
    EXPECT_EQ(ConvertTo<int>(result.Patches[0].Changes[0].Value), 3);
    EXPECT_FALSE(result.Patches[1].Comment);
    ASSERT_EQ(result.Patches[1].Changes.size(), 2u);
    EXPECT_EQ(result.Patches[1].Changes[0].Path, "/a");
    EXPECT_EQ(ConvertTo<int>(result.Patches[1].Changes[0].Value), 4);
    EXPECT_EQ(ConvertTo<IMapNodePtr>(result.Patches[1].Changes[1].Value)->GetChildValueOrThrow<int>("x"), 5);

    auto roundTrip = ConvertTo<TGetPipelineDynamicSpecPatchesResult>(ConvertToYsonString(result));
    EXPECT_EQ(roundTrip.Patches[0].Timestamp, result.Patches[0].Timestamp);
    EXPECT_EQ(roundTrip.Patches[1].Changes[1].Path, "/b");
}

TEST(TFlowClientTest, DefaultDynamicSpecReadUsesLegacyArguments)
{
    auto client = New<TRecordingFlowClient>();
    client->Response = TYsonString(TStringBuf("{version=11;spec={x=2;};}"));
    auto result = WaitFor(client->IFlowClient::GetPipelineDynamicSpec("//pipeline"))
        .ValueOrThrow();
    EXPECT_EQ(client->Command, "get-pipeline-dynamic-spec");
    EXPECT_EQ(ConvertTo<IMapNodePtr>(client->Argument)->GetChildCount(), 0);
    EXPECT_EQ(result.Version, TVersion(11));
    EXPECT_EQ(ConvertTo<IMapNodePtr>(result.Spec)->GetChildValueOrThrow<int>("x"), 2);
    EXPECT_FALSE(result.Blame);
}

TEST(TFlowClientTest, PatchValidationErrorsAreReturnedInFutures)
{
    auto client = New<TRecordingFlowClient>();
    EXPECT_FALSE(WaitFor(client->PatchPipelineDynamicSpec("//pipeline", {})).IsOK());
    EXPECT_FALSE(WaitFor(client->CancelPipelineDynamicSpecPatch("//pipeline", TVersion(7))).IsOK());
    EXPECT_FALSE(WaitFor(client->ResetPipelineDynamicSpec("//pipeline")).IsOK());

    TPatchPipelineDynamicSpecOptions options;
    options.ExpectedVersion = TVersion(11);
    EXPECT_FALSE(WaitFor(client->PatchPipelineDynamicSpec("//pipeline", options)).IsOK());
    options.SetPath = "/x";
    options.SetValue = TYsonString(TStringBuf("8"));
    options.SetChanges = {{"/y", TYsonString(TStringBuf("9"))}};
    options.Comment = "Conflicting arguments";
    EXPECT_FALSE(WaitFor(client->PatchPipelineDynamicSpec("//pipeline", options)).IsOK());
    EXPECT_TRUE(client->Command.empty());
}

TEST(TFlowClientTest, DynamicSpecViewAndBlameUseFlowCommand)
{
    auto client = New<TRecordingFlowClient>();
    client->Response = TYsonString(TStringBuf("{version=11;spec={x=2;};blame={x=7;};}"));
    TGetPipelineDynamicSpecOptions options;
    options.View = EDynamicSpecView::Base;
    options.Blame = true;
    options.Timeout = TDuration::Seconds(17);
    auto result = WaitFor(client->IFlowClient::GetPipelineDynamicSpec("//pipeline", options))
        .ValueOrThrow();
    auto argument = ConvertTo<IMapNodePtr>(client->Argument);
    EXPECT_EQ(client->Command, "get-pipeline-dynamic-spec");
    EXPECT_EQ(argument->GetChildValueOrThrow<std::string>("view"), "base");
    EXPECT_TRUE(argument->GetChildValueOrThrow<bool>("blame"));
    EXPECT_EQ(client->Options.Timeout, options.Timeout);
    EXPECT_EQ(result.Version, TVersion(11));
    EXPECT_EQ(ConvertTo<IMapNodePtr>(result.Spec)->GetChildValueOrThrow<int>("x"), 2);
    EXPECT_EQ(ConvertTo<IMapNodePtr>(result.Blame)->GetChildValueOrThrow<int>("x"), 7);
}

TEST(TFlowClientTest, PatchWritesRejectEmptyComments)
{
    auto client = New<TRecordingFlowClient>();
    for (const auto& comment : std::vector<std::optional<std::string>>{std::nullopt, ""}) {
        EXPECT_THROW(ValidatePipelineDynamicSpecPatchComment(comment), TErrorException);
        TPatchPipelineDynamicSpecOptions options;
        options.Comment = comment;
        options.ExpectedVersion = TVersion(11);
        options.SetPath = "/x";
        options.SetValue = TYsonString(TStringBuf("8"));
        auto result = client->PatchPipelineDynamicSpec("//pipeline", options);
        EXPECT_FALSE(WaitFor(result).IsOK());
        EXPECT_TRUE(client->Command.empty());
    }
    EXPECT_NO_THROW(ValidatePipelineDynamicSpecPatchComment("  Причина 😀  "));
    EXPECT_NO_THROW(ValidatePipelineDynamicSpecPatchComment("\u200b"));
    EXPECT_NO_THROW(ValidatePipelineDynamicSpecPatchComment("\x1c\x1d\x1e\x1f"));
    EXPECT_NO_THROW(ValidatePipelineDynamicSpecPatchComment("\u0085 Причина 😀 \u200b"));
}

TEST(TFlowClientTest, PatchCommandsCarryCasAndAudit)
{
    auto client = New<TRecordingFlowClient>();
    TPatchPipelineDynamicSpecOptions patchOptions;
    patchOptions.SetChanges = {
        {"/x", TYsonString(TStringBuf("8"))},
        {"/y", TYsonString(TStringBuf("9"))},
    };
    patchOptions.Comment = "update both limits";
    patchOptions.ExpectedVersion = TVersion(12);
    client->Response = TYsonString(TStringBuf("{version=13;patch_id=13;}"));
    auto patchResult = WaitFor(client->PatchPipelineDynamicSpec("//pipeline", patchOptions))
        .ValueOrThrow();
    auto argument = ConvertTo<IMapNodePtr>(client->Argument);
    EXPECT_EQ(client->Command, "patch-pipeline-dynamic-spec");
    auto changes = argument->GetChildOrThrow("set_changes")->AsList()->GetChildren();
    ASSERT_EQ(changes.size(), 2u);
    EXPECT_EQ(changes[0]->AsMap()->GetChildValueOrThrow<std::string>("path"), "/x");
    EXPECT_EQ(changes[1]->AsMap()->GetChildValueOrThrow<int>("value"), 9);
    EXPECT_EQ(argument->GetChildValueOrThrow<std::string>("comment"), *patchOptions.Comment);
    EXPECT_EQ(patchResult.PatchId, TVersion(13));
    client->Response = TYsonString(TStringBuf("{version=14;}"));

    TCancelPipelineDynamicSpecPatchOptions cancelPatchOptions;
    cancelPatchOptions.ExpectedVersion = TVersion(12);
    WaitFor(client->CancelPipelineDynamicSpecPatch("//pipeline", TVersion(7), cancelPatchOptions))
        .ThrowOnError();
    argument = ConvertTo<IMapNodePtr>(client->Argument);
    EXPECT_EQ(client->Command, "cancel-pipeline-dynamic-spec-patch");
    EXPECT_EQ(argument->GetChildValueOrThrow<TVersion>("patch"), TVersion(7));
    EXPECT_EQ(argument->GetChildValueOrThrow<TVersion>("expected_version"), TVersion(12));

    TResetPipelineDynamicSpecOptions resetOptions;
    resetOptions.ExpectedVersion = TVersion(14);
    WaitFor(client->ResetPipelineDynamicSpec("//pipeline", resetOptions))
        .ThrowOnError();
    argument = ConvertTo<IMapNodePtr>(client->Argument);
    EXPECT_EQ(client->Command, "reset-pipeline-dynamic-spec");
    EXPECT_EQ(argument->GetChildValueOrThrow<TVersion>("expected_version"), TVersion(14));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi

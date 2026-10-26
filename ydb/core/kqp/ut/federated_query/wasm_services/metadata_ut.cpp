#include <ydb/core/kqp/wasm_services/connection.h>

#include <ydb/library/yql/providers/function/provider/dq_function_provider.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>

#include <barrier>
#include <stdexcept>
#include <thread>

namespace NKikimr::NKqp {
namespace {
TIntrusivePtr<NYql::TKikimrTableMetadata> Connection(ui64 id, ui64 version, TString endpoint = "https://old.example") {
    auto metadata = MakeIntrusive<NYql::TKikimrTableMetadata>("cluster", "/Root/service");
    metadata->PathId = NYql::TKikimrPathId(42, id);
    metadata->SchemaVersion = version;
    metadata->Kind = NYql::EKikimrTableKind::External;
    metadata->TableType = NYql::ETableType::Unknown;
    NKikimrSchemeOp::TExternalDataSourceDescription description;
    description.SetSourceType("ExternalService");
    description.SetLocation(endpoint);
    description.MutableAuth()->MutableToken()->SetTokenSecretName("/Root/service_secret");
    (*description.MutableProperties()->MutableProperties())["protocol"] = "HTTP";
    metadata->ExternalSource = NYql::TExternalDataSource::CreateFromDescription(description, "/Root/service");
    return metadata;
}

using namespace NYql;
using namespace NYql::NDqFunction;

struct TPendingGateway: IDqFunctionGateway {
    NThreading::TFuture<TDqFunctionDescription> Future;

    explicit TPendingGateway(NThreading::TFuture<TDqFunctionDescription> future)
        : Future(std::move(future))
    {
    }

    NThreading::TFuture<TDqFunctionDescription> ResolveFunction(const TString&, const TString&) override {
        return Future;
    }
};

void ResolveConcurrently(bool errors, bool rewind) {
    constexpr size_t Count = 256;
    constexpr size_t Threads = 16;
    for (size_t round = 0; round < 8; ++round) {
        auto state = MakeIntrusive<TDqFunctionState>();
        std::vector<NThreading::TPromise<TDqFunctionDescription>> promises;
        for (size_t i = 0; i < Count; ++i) {
            promises.push_back(NThreading::NewPromise<TDqFunctionDescription>());
            state->FunctionsResolver->AddFunction("service", "Call", ToString(i));
        }
        state->GatewayFactory = MakeIntrusive<TDqFunctionGatewayFactory>();
        state->GatewayFactory->Register("service", [&](const auto&, const TString& connection) {
            return std::make_shared<TPendingGateway>(promises.at(FromString<size_t>(connection)).GetFuture());
        });
        auto source = CreateDqFunctionDataSource(state);
        auto& loader = source->GetLoadTableMetadataTransformer();
        TExprContext ctx;
        // Check every callback, independently of the production diagnostic cap.
        ctx.IssueManager.SetIssueCountLimit(Count);
        auto input = ctx.NewAtom(TPositionHandle{}, "query");
        TExprNode::TPtr output;
        UNIT_ASSERT_VALUES_EQUAL(loader.Transform(input, output, ctx).Level, IGraphTransformer::TStatus::Async);
        auto ready = loader.GetAsyncFuture(*input);
        if (rewind) {
            loader.Rewind();
        }
        std::barrier start(Threads);
        std::vector<std::thread> workers;
        for (size_t thread = 0; thread < Threads; ++thread) {
            workers.emplace_back([&, thread] {
                start.arrive_and_wait();
                for (size_t i = thread; i < Count; i += Threads) {
                    if (errors && i % 2) {
                        promises[i].SetException(std::make_exception_ptr(std::runtime_error("resolution " + std::to_string(i))));
                    } else {
                        promises[i].SetValue(TDqFunctionDescription{
                            .Type = "service", .FunctionName = "Call", .Connection = ToString(i), .InvokeUrl = ToString(i) });
                    }
                }
            });
        }
        for (auto& worker : workers) {
            worker.join();
        }
        ready.GetValueSync();
        UNIT_ASSERT(state->FunctionsDescription.empty());
        if (rewind) {
            continue;
        }
        const auto result = loader.ApplyAsyncChanges(input, output, ctx);
        UNIT_ASSERT_VALUES_EQUAL(result.Level, errors ? IGraphTransformer::TStatus::Error : IGraphTransformer::TStatus::Ok);
        if (errors) {
            UNIT_ASSERT_VALUES_EQUAL(ctx.IssueManager.GetIssues().Size(), Count / 2);
            UNIT_ASSERT(state->FunctionsDescription.empty());
        } else {
            UNIT_ASSERT_VALUES_EQUAL(state->FunctionsDescription.size(), Count);
            for (const auto& description : state->FunctionsDescription) {
                UNIT_ASSERT_VALUES_EQUAL(description.Connection, description.InvokeUrl);
            }
        }
    }
}
}   // namespace

Y_UNIT_TEST_SUITE(KqpWasmMetadataRegressions) {
    Y_UNIT_TEST(ConnectionSnapshotRejectsReplacement) {
        const auto resolved = Connection(1, 7);
        const auto serialized = SerializeWasmServiceConnection(*resolved);
        ValidateWasmServiceConnection(serialized, *Connection(1, 7));
        // DROP/CREATE may reuse the version and properties, but never the path id.
        UNIT_ASSERT_EXCEPTION_CONTAINS(ValidateWasmServiceConnection(serialized, *Connection(2, 7)), yexception, "connection changed");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ValidateWasmServiceConnection(serialized, *Connection(1, 8)), yexception, "connection changed");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            ValidateWasmServiceConnection(serialized, *Connection(1, 7, "https://new.example")), yexception, "connection changed");
    }
    Y_UNIT_TEST(ConcurrentFunctionResults) {
        ResolveConcurrently(false, false);
    }
    Y_UNIT_TEST(ConcurrentFunctionErrors) {
        ResolveConcurrently(true, false);
    }
    Y_UNIT_TEST(RewindDuringConcurrentResolution) {
        ResolveConcurrently(true, true);
    }
}
}   // namespace NKikimr::NKqp

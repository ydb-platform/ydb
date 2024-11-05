#pragma once

#include "flat_local_tx_factory.h"
#include "flat_local_minikql_host.h"
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>
#include <ydb/core/client/minikql_compile/compile_context.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NMiniKQL {

class TLocalDbSchemeResolver : public NYql::IDbSchemeResolver {
public:
    TLocalDbSchemeResolver(const NTable::TScheme& scheme, ui64 tabletId)
        : Scheme(scheme)
        , TabletId(tabletId) {}

    virtual NThreading::TFuture<TTableResults> ResolveTables(const TVector<TTable>& tables) override {
        TTableResults results;
        results.reserve(tables.size());

        for (auto& table : tables) {
            TTableResult result(TTableResult::Ok);

            const ui32* tableId = Scheme.TableNames.FindPtr(table.TableName);
            if (!tableId) {
                result = TTableResult(TTableResult::Error, "Unknown table " + table.TableName);
            } else {
                const auto *tableInfo = Scheme.Tables.FindPtr(*tableId);
                Y_ABORT_UNLESS(tableInfo);

                result.KeyColumnCount = tableInfo->KeyColumns.size();
                result.Table = table;
                result.TableId = new TTableId(TabletId, *tableId);

                for (const auto& column : table.ColumnNames) {
                    const ui32* columnId = tableInfo->ColumnNames.FindPtr(column);
                    if (!columnId) {
                        result = TTableResult(TTableResult::Error, "Unknown column " + table.TableName + ":" + column);
                        break;
                    }

                    const auto *columnInfo = tableInfo->Columns.FindPtr(*columnId);
                    Y_ABORT_UNLESS(columnInfo);

                    auto nullConstraint = columnInfo->NotNull ? EColumnTypeConstraint::NotNull : EColumnTypeConstraint::Nullable;
                    auto insertResult = result.Columns.insert(std::make_pair(column, IDbSchemeResolver::TTableResult::TColumn
                    {*columnId, (i32)columnInfo->KeyOrder, columnInfo->PType, 0, nullConstraint}));
                    Y_ABORT_UNLESS(insertResult.second);
                }
            }

            results.push_back(result);
        }

        return NThreading::MakeFuture<TTableResults>(results);
    }

    virtual void ResolveTables(const TVector<TTable>& tables, NActors::TActorId responseTo) override {
        Y_UNUSED(tables);
        Y_UNUSED(responseTo);
        Y_ABORT("Not implemented for local resolve.");
    }
private:
    const NTable::TScheme& Scheme;
    const ui64 TabletId;
};

class TFlatLocalMiniKQL : public NTabletFlatExecutor::ITransaction {
    ui64 TabletId = Max<ui64>();
    const TActorId Sender;
    const TLocalMiniKQLProgram SourceProgram;
    const TMiniKQLFactory* const Factory;

    TString SerializedMiniKQLProgram;
    TString SerializedMiniKQLParams;

    TAutoPtr<NYql::TMiniKQLCompileResult> ProgramCompileResult;

    IEngineFlat::EResult EngineResultStatusCode;
    IEngineFlat::EStatus EngineResponseStatus;
    TAutoPtr<NKikimrMiniKQL::TResult> EngineEvaluatedResponse;
    ui64 PageFaultCount = 0;

    bool ParseProgram(TStringBuf program, NYql::TIssues &errors, NYql::TExprContainer &expr) {
        NYql::TAstParseResult astResult = NYql::ParseAst(program);
        if (!astResult.Root) {
            errors = astResult.Issues;
            return false;
        }
        expr.Context.IssueManager.AddIssues(astResult.Issues);

        if (!NYql::CompileExpr(*astResult.Root, expr.Root, expr.Context, nullptr, nullptr)) {
            errors = expr.Context.IssueManager.GetIssues();
            return false;
        }

        return true;
    }

    bool PrepareParams(TTransactionContext &txc, const TAppData *appData) {
        Y_UNUSED(txc);
        if (SourceProgram.Params.Binary) {
            SerializedMiniKQLParams = SourceProgram.Params.Binary;
            return true;
        }

        if (SourceProgram.Params.Text) {
            ProgramCompileResult = new NYql::TMiniKQLCompileResult();

            NYql::TExprContainer::TPtr expr  = new NYql::TExprContainer();
            NYql::TMiniKQLCompileResult parseResult;
            if (!ParseProgram(SourceProgram.Params.Text, parseResult.Errors, *expr)) {
                ProgramCompileResult->Errors.AddIssues(parseResult.Errors);
                return false;
            }

            TAlignedPagePoolCounters counters(appData->Counters, "local_tx");
            TScopedAlloc alloc(__LOCATION__, counters, appData->FunctionRegistry->SupportsSizedAllocators());
            TTypeEnvironment typeEnv(alloc);
            auto future = ConvertToMiniKQL(expr, appData->FunctionRegistry, &typeEnv, nullptr);
            future.Wait();
            NYql::TConvertResult compileResult = future.GetValue();

            if (!compileResult.Errors.Empty()) {
                ProgramCompileResult->Errors.AddIssues(compileResult.Errors);
                return false;
            }
        }

        return true;
    }

    bool PrepareProgram(TTransactionContext &txc, const TAppData *appData) {
        // simple case - everything prepared for us and no params
        if (SourceProgram.Program.Binary) {
            SerializedMiniKQLProgram = SourceProgram.Program.Binary;
            return true;
        }

        // so we must prepare program
        ProgramCompileResult = new NYql::TMiniKQLCompileResult();

        NYql::TExprContainer::TPtr expr  = new NYql::TExprContainer();
        NYql::TMiniKQLCompileResult parseResult;
        if (!ParseProgram(SourceProgram.Program.Text, parseResult.Errors, *expr)) {
            ProgramCompileResult->Errors.AddIssues(parseResult.Errors);
            return false;
        }

        TAlignedPagePoolCounters counters(appData->Counters, "local_tx");
        TScopedAlloc alloc(__LOCATION__, counters, appData->FunctionRegistry->SupportsSizedAllocators());
        TTypeEnvironment typeEnv(alloc);
        TLocalDbSchemeResolver dbResolver(txc.DB.GetScheme(), TabletId);
        const auto unguard = Unguard(alloc);
        auto future = ConvertToMiniKQL(expr, appData->FunctionRegistry, &typeEnv, &dbResolver);
        future.Wait();
        NYql::TConvertResult compileResult = future.GetValue();
        if (!compileResult.Errors.Empty()) {
            ProgramCompileResult->Errors.AddIssues(compileResult.Errors);
            return false;
        }

        ProgramCompileResult->CompiledProgram = SerializeRuntimeNode(compileResult.Node, typeEnv);
        SerializedMiniKQLProgram = ProgramCompileResult->CompiledProgram;
        return true;
    }

    void ClearResponse() {
        EngineResultStatusCode = IEngineFlat::EResult::Unknown;
        EngineResponseStatus = IEngineFlat::EStatus::Unknown;
        EngineEvaluatedResponse.Destroy();
    }

    bool MakeCompileResponse(const TActorContext &ctx) {
        TAutoPtr<TEvTablet::TEvLocalMKQLResponse> response = new TEvTablet::TEvLocalMKQLResponse();
        auto &record = response->Record;
        record.SetOrigin(TabletId);

        if (ProgramCompileResult) {
            auto *compileResults = record.MutableCompileResults();
            if (ProgramCompileResult->Errors.Empty()) {
                record.SetStatus(NKikimrProto::OK);
                compileResults->SetCompiledProgram(ProgramCompileResult->CompiledProgram);
            } else {
                NYql::IssuesToMessage(ProgramCompileResult->Errors, compileResults->MutableProgramCompileErrors());
                record.SetStatus(NKikimrProto::ERROR);
            }
        } else {
            record.SetStatus(NKikimrProto::ERROR);
        }

        ctx.Send(Sender, response.Release());
        return true;
    }

    bool MakeResponse(IEngineFlat *engine, const TActorContext &ctx) {
        TAutoPtr<TEvTablet::TEvLocalMKQLResponse> response = new TEvTablet::TEvLocalMKQLResponse();
        auto &record = response->Record;
        record.SetOrigin(TabletId);

        record.SetStatus((EngineResultStatusCode == IEngineFlat::EResult::Ok && EngineResponseStatus != IEngineFlat::EStatus::Error) ? NKikimrProto::OK : NKikimrProto::ERROR);
        if (EngineResultStatusCode != IEngineFlat::EResult::Unknown)
            record.SetEngineStatus(static_cast<ui32>(EngineResultStatusCode));
        if (EngineResponseStatus != IEngineFlat::EStatus::Unknown)
            record.SetEngineResponseStatus(static_cast<ui32>(EngineResponseStatus));

        if (EngineEvaluatedResponse)
            *record.MutableExecutionEngineEvaluatedResponse() = *EngineEvaluatedResponse;

        if (engine && engine->GetErrors())
            record.SetMiniKQLErrors(engine->GetErrors());

        ctx.Send(Sender, response.Release());

        EngineResultStatusCode = IEngineFlat::EResult::Unknown;
        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override {
        ClearResponse();

        TabletId = txc.Tablet;

        const TAppData *appData = AppData(ctx);
        const auto functionRegistry = appData->FunctionRegistry;

        if (!SerializedMiniKQLProgram) {
            if (!PrepareProgram(txc, appData))
                return MakeCompileResponse(ctx);

            if (SourceProgram.CompileOnly)
                return MakeCompileResponse(ctx);
        }

        if (!SerializedMiniKQLParams) {
            if (!PrepareParams(txc, appData))
                return MakeResponse(nullptr, ctx);
        }

        try {
            TAlignedPagePoolCounters poolCounters(appData->Counters, "local_tx");

            TEngineFlatSettings proxySettings(
                IEngineFlat::EProtocol::V1,
                functionRegistry,
                *TAppData::RandomProvider, *TAppData::TimeProvider,
                nullptr, poolCounters
            );
            proxySettings.EvaluateResultType = true;
            proxySettings.EvaluateResultValue = true;

            TAutoPtr<IEngineFlat> proxyEngine = CreateEngineFlat(proxySettings);
            EngineResultStatusCode = proxyEngine->SetProgram(SerializedMiniKQLProgram, SerializedMiniKQLParams);
            if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                return MakeResponse(proxyEngine.Get(), ctx);

            for (auto &key : proxyEngine->GetDbKeys()) {
                key->Status = TKeyDesc::EStatus::Ok;

                auto partitions = std::make_shared<TVector<TKeyDesc::TPartitionInfo>>();
                partitions->push_back(TKeyDesc::TPartitionInfo(TabletId));
                key->Partitioning = partitions;

                for (const auto &x : key->Columns) {
                    key->ColumnInfos.push_back({x.Column, x.ExpectedType, 0, TKeyDesc::EStatus::Ok}); // type-check
                }
            }

            EngineResultStatusCode = proxyEngine->PrepareShardPrograms(IEngineFlat::TShardLimits(1, 0));
            if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                return MakeResponse(proxyEngine.Get(), ctx);

            const ui32 affectedShardCount = proxyEngine->GetAffectedShardCount();

            if (affectedShardCount == 0) {
                proxyEngine->AfterShardProgramsExtracted();
            } else {
                Y_ABORT_UNLESS(affectedShardCount == 1);

                IEngineFlat::TShardData shardData;
                EngineResultStatusCode = proxyEngine->GetAffectedShard(0, shardData);
                if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                    return MakeResponse(proxyEngine.Get(), ctx);

                const TString shardProgram = shardData.Program;
                proxyEngine->AfterShardProgramsExtracted();

                TEngineHostCounters hostCounters;
                TLocalMiniKQLHost host(txc.DB, hostCounters, TEngineHostSettings(TabletId, false), Factory);
                TEngineFlatSettings engineSettings(
                    IEngineFlat::EProtocol::V1,
                    functionRegistry,
                    *TAppData::RandomProvider, *TAppData::TimeProvider,
                    &host, poolCounters
                );
                TAutoPtr<IEngineFlat> engine = CreateEngineFlat(engineSettings);
                EngineResultStatusCode = engine->AddProgram(TabletId, shardProgram);
                if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                    return MakeResponse(engine.Get(), ctx);

                IEngineFlat::TValidationInfo validationInfo;
                EngineResultStatusCode = engine->Validate(validationInfo);
                if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                    return MakeResponse(engine.Get(), ctx);

                EngineResultStatusCode = engine->PinPages(PageFaultCount);
                if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                    return MakeResponse(engine.Get(), ctx);

                EngineResultStatusCode = engine->PrepareOutgoingReadsets();
                if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                    return MakeResponse(engine.Get(), ctx);

                Y_ABORT_UNLESS(engine->GetOutgoingReadsetsCount() == 0);
                engine->AfterOutgoingReadsetsExtracted();

                EngineResultStatusCode = engine->PrepareIncomingReadsets();
                if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                    return MakeResponse(engine.Get(), ctx);

                Y_ABORT_UNLESS(engine->GetExpectedIncomingReadsetsCount() == 0);

                EngineResultStatusCode = engine->Execute();
                if (EngineResultStatusCode != IEngineFlat::EResult::Ok)
                    return MakeResponse(engine.Get(), ctx);

                const TString shardEngineReply = engine->GetShardReply(TabletId);
                proxyEngine->AddShardReply(TabletId, shardEngineReply);
                proxyEngine->FinalizeOriginReplies(TabletId);
            }

            proxyEngine->BuildResult();
            EngineResponseStatus = proxyEngine->GetStatus();

            if (EngineResponseStatus == IEngineFlat::EStatus::Complete || EngineResponseStatus == IEngineFlat::EStatus::Aborted) {
                EngineEvaluatedResponse = new NKikimrMiniKQL::TResult();
                proxyEngine->FillResultValue(*EngineEvaluatedResponse);
            } else
                return MakeResponse(proxyEngine.Get(), ctx);

            return true;
        } catch (const TNotReadyTabletException& ex) {
            Y_UNUSED(ex);
            ++PageFaultCount;
            return false;
        } catch (...) {
            Y_ABORT("there must be no leaked exceptions");
        }
    }

    void Complete(const TActorContext &ctx) override {
        if (EngineResultStatusCode != IEngineFlat::EResult::Unknown)
            MakeResponse(nullptr, ctx);
    }

public:
    TFlatLocalMiniKQL(
            TActorId sender,
            const TLocalMiniKQLProgram &program,
            const TMiniKQLFactory* factory)
        : Sender(sender)
        , SourceProgram(program)
        , Factory(factory)
    {}
};

}}

#include "dq_yt_reader.h"

#include "dq_yt_reader_impl.h"
#include "dq_yt_block_reader.h"
#include "dq_yt_rpc_reader.h"

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

class TDqYtReadWrapperHttp : public TDqYtReadWrapperBase<TDqYtReadWrapperHttp, TFileInputState> {
public:
using TInputType = NYT::TRawTableReaderPtr;
    TDqYtReadWrapperHttp(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups, TType* itemType, const TVector<TString>& tableNames,
        TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight, size_t timeout,
        const TVector<ui64>& tableOffsets)
            : TDqYtReadWrapperBase<TDqYtReadWrapperHttp, TFileInputState>(ctx, clusterName, token,
                inputSpec, samplingSpec, inputGroups, itemType, tableNames, std::move(tables),
                jobStats, inflight, timeout, tableOffsets) {}

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        TVector<NYT::TRawTableReaderPtr> rawReaders;

        NYT::TCreateClientOptions createOpts;
        if (Token) {
            createOpts.Token(Token);
        }

        auto client = NYT::CreateClient(ClusterName, createOpts);
        NYT::TTableReaderOptions readerOptions;
        if (!SamplingSpec.IsUndefined()) {
            readerOptions.Config(SamplingSpec);
        }

        for (auto [richYPath, format]: Tables) {
            NYT::TRawTableReaderPtr reader;
            const int lastAttempt = NYT::TConfig::Get()->ReadRetryCount - 1;
            for (int attempt = 0; attempt <= lastAttempt; ++attempt) {
                try {
                    if (richYPath.TransactionId_) {
                        auto transaction = client->AttachTransaction(richYPath.TransactionId_.GetRef(), NYT::TAttachTransactionOptions().AutoPingable(true));
                        richYPath.TransactionId_.Clear();
                        reader = transaction->CreateRawReader(richYPath, format, readerOptions.CreateTransaction(false));
                    } else {
                        reader = client->CreateRawReader(richYPath, format, readerOptions.CreateTransaction(true));
                    }
                    break;
                } catch (const NYT::TErrorResponse& e) {
                    Cerr << "Error creating reader for " << richYPath.Path_ << ": " << e.what();
                    // Already retried inside CreateRawReader
                    throw;
                } catch (const yexception& e) {
                    Cerr << "Error creating reader for " << richYPath.Path_ << ": " << e.what();
                    if (attempt == lastAttempt) {
                        throw;
                    }
                    NYT::NDetail::TWaitProxy::Get()->Sleep(NYT::TConfig::Get()->RetryInterval);
                }
            }
            rawReaders.push_back(reader);
        }

        state = ctx.HolderFactory.Create<TDqYtReadWrapperBase<TDqYtReadWrapperHttp, TFileInputState>::TState>(Specs, ctx.HolderFactory, std::move(rawReaders), 4, 4_MB);
    }
};

IComputationNode* WrapDqYtRead(TCallable& callable, NKikimr::NMiniKQL::IStatsRegistry* jobStats, const TComputationNodeFactoryContext& ctx, bool useBlocks) {
    MKQL_ENSURE(callable.GetInputsCount() == 8 || callable.GetInputsCount() == 9, "Expected 8 or 9 arguments.");

    TString clusterName(AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().AsStringRef());
    TString tokenName(AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().AsStringRef());
    TString inputSpec(AS_VALUE(TDataLiteral, callable.GetInput(2))->AsValue().AsStringRef());
    TString samplingSpec(AS_VALUE(TDataLiteral, callable.GetInput(3))->AsValue().AsStringRef());
    TString token;
    if (auto sec = ctx.SecureParamsProvider) {
        NUdf::TStringRef val;
        if (sec->GetSecureParam(tokenName, val)) {
            token = val;
        }
    }

    TVector<ui32> inputGroups;
    TVector<TString> tableNames;
    TVector<std::pair<NYT::TRichYPath, NYT::TFormat>> tables; // richpath, skiff
    TListLiteral* groupList = AS_VALUE(TListLiteral, callable.GetInput(4));
    TVector<ui64> tableOffsets;
    for (ui32 grp = 0; grp < groupList->GetItemsCount(); ++grp) {
        TListLiteral* tableList = AS_VALUE(TListLiteral, groupList->GetItems()[grp]);
        for (ui32 i = 0; i < tableList->GetItemsCount(); ++i) {
            TTupleLiteral* tableTuple = AS_VALUE(TTupleLiteral, tableList->GetItems()[i]);
            YQL_ENSURE(tableTuple->GetValuesCount() == 4);
            tableNames.emplace_back(AS_VALUE(TDataLiteral, tableTuple->GetValue(0))->AsValue().AsStringRef());
            inputGroups.push_back(grp);

            NYT::TRichYPath richYPath;
            NYT::Deserialize(richYPath, NYT::NodeFromYsonString(TString(AS_VALUE(TDataLiteral, tableTuple->GetValue(1))->AsValue().AsStringRef())));
            tables.emplace_back(richYPath, NYT::TFormat(NYT::NodeFromYsonString(AS_VALUE(TDataLiteral, tableTuple->GetValue(2))->AsValue().AsStringRef())));
            tableOffsets.push_back(AS_VALUE(TDataLiteral, tableTuple->GetValue(3))->AsValue().Get<ui64>());
        }
    }
    if (1 == groupList->GetItemsCount()) {
        inputGroups.clear();
    }
    size_t timeout(AS_VALUE(TDataLiteral, callable.GetInput(7))->AsValue().Get<size_t>());
#ifdef __linux__
    size_t inflight(AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<size_t>());
    if (inflight) {
        if (useBlocks) {
            return CreateDqYtReadBlockWrapper(ctx, clusterName, token,
                NYT::NodeFromYsonString(inputSpec), samplingSpec ? NYT::NodeFromYsonString(samplingSpec) : NYT::TNode(),
                inputGroups, static_cast<TType*>(callable.GetInput(5).GetNode()), tableNames, std::move(tables), jobStats,
                inflight, timeout, tableOffsets);
        } else {
            return new TDqYtReadWrapperBase<TDqYtReadWrapperRPC, TParallelFileInputState>(ctx, clusterName, token,
                NYT::NodeFromYsonString(inputSpec), samplingSpec ? NYT::NodeFromYsonString(samplingSpec) : NYT::TNode(),
                inputGroups, static_cast<TType*>(callable.GetInput(5).GetNode()), tableNames, std::move(tables), jobStats,
                inflight, timeout, tableOffsets);
        }
    } else {
        YQL_ENSURE(!useBlocks);
        return new TDqYtReadWrapperBase<TDqYtReadWrapperHttp, TFileInputState>(ctx, clusterName, token,
            NYT::NodeFromYsonString(inputSpec), samplingSpec ? NYT::NodeFromYsonString(samplingSpec) : NYT::TNode(),
            inputGroups, static_cast<TType*>(callable.GetInput(5).GetNode()), tableNames, std::move(tables), jobStats,
            inflight, timeout, tableOffsets);
    }
#else
    YQL_ENSURE(!useBlocks);
    return new TDqYtReadWrapperBase<TDqYtReadWrapperHttp, TFileInputState>(ctx, clusterName, token,
        NYT::NodeFromYsonString(inputSpec), samplingSpec ? NYT::NodeFromYsonString(samplingSpec) : NYT::TNode(),
        inputGroups, static_cast<TType*>(callable.GetInput(5).GetNode()), tableNames, std::move(tables), jobStats, 0, timeout,
        tableOffsets);
#endif
}

} // NYql

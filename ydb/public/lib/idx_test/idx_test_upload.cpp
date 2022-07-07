#include "idx_test.h"
#include "idx_test_common.h"
#include "idx_test_data_provider.h"

#include <util/string/printf.h>
#include <util/system/mutex.h>
#include <library/cpp/threading/future/future.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

using namespace NYdb;
using namespace NYdb::NTable;

namespace NIdxTest {

class TUploader : public IUploader {
public:
    TUploader(TTableClient&& client, const TString& table, const TUploaderParams& params)
        : Client_(std::move(client))
        , Table_(table)
        , Params_(params)
    {
    }

    void Run(IDataProvider::TPtr dataProvider) override {
        CreateTable(dataProvider->GetTableSchema());
        CreateQuery(dataProvider->GetRowType());
        UploadData(dataProvider.get());
    }

private:
    enum class EUploadStatus {
        InProgress,
        Done
    };

    void CreateTable(TTableDescription tableDesc) {
        ThrowOnError(Client_.RetryOperationSync([this, tableDesc](TSession session) {
            TTableDescription desc(tableDesc);
            return session.CreateTable(
                Table_,
                std::move(desc),
                TCreateTableSettings()
                    .PartitioningPolicy(
                        TPartitioningPolicy()
                            .UniformPartitions(Params_.ShardsCount)
                    )
                    .ClientTimeout(TDuration::Seconds(5))).GetValueSync();
        }));
    }

    void CreateQuery(NYdb::TType rowType) {
        auto rowsTypeString = NYdb::FormatType(rowType);

        Query_ = Sprintf("DECLARE %s AS List<%s>; REPLACE INTO `%s` SELECT * FROM AS_TABLE(%s);",
            ParamName_.c_str(),
            rowsTypeString.c_str(),
            Table_.c_str(),
            ParamName_.c_str());
    }

    TMaybe<NYdb::TParams> CreateParams(IDataProvider* dataProvider, ui32 id) {
        const auto& mayBeItems = dataProvider->GetBatch(id);
        if (!mayBeItems) {
            return {};
        }
        return ::NIdxTest::CreateParamsAsList(mayBeItems.GetRef(), ParamName_);
    }

    std::function<NYdb::TAsyncStatus(TSession session)> CreateUploadFn(IDataProvider* dataProvider, ui32 id) {
        return [this, dataProvider, id](TSession session) -> TAsyncStatus {

            if (FinishedByError)
                return NThreading::MakeFuture<NYdb::TStatus>(TStatus(EStatus::INTERNAL_ERROR, NYql::TIssues()));

            auto maybeParamsList = CreateParams(dataProvider, id);
            if (!maybeParamsList) {
                UploadStatuses_[id] = EUploadStatus::Done;
                return NThreading::MakeFuture<NYdb::TStatus>(TStatus(EStatus::SUCCESS, NYql::TIssues()));
            }

            auto paramsList = maybeParamsList.GetRef();
            return session.PrepareDataQuery(Query_).Apply(
                [paramsList](TAsyncPrepareQueryResult result) -> NYdb::TAsyncStatus {
                    auto prepareResult = result.ExtractValue();
                    if (!prepareResult.IsSuccess())
                        return NThreading::MakeFuture<NYdb::TStatus>(prepareResult);

                    auto dataQuery = prepareResult.GetQuery();
                    return dataQuery.Execute(TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(),
                                             paramsList).Apply(
                        [](TAsyncDataQueryResult asyncResult) {
                            auto result = asyncResult.ExtractValue();
                            return NThreading::MakeFuture<NYdb::TStatus>(result);
                        });
                });
        };
    }

    TAsyncStatus RunUploadTask(IDataProvider* dataProvider, ui32 id, std::function<TAsyncStatus(TSession session)> uploadFn) {
        return Client_.RetryOperation(uploadFn).Apply(
            [dataProvider, id, uploadFn, this](TAsyncStatus asyncStatus) {
            auto status = asyncStatus.GetValue();
            if (!status.IsSuccess() || UploadStatuses_[id] == EUploadStatus::Done) {
                return NThreading::MakeFuture<TStatus>(status);
            }
            return RunUploadTask(dataProvider, id, uploadFn);
        });
    }

    void UploadData(IDataProvider* dataProvider) {
        for (ui32 shard = 0; shard < Params_.ShardsCount; shard++) {
            UploadStatuses_.push_back(EUploadStatus::InProgress);
        }

        TVector<TAsyncStatus> resultFutures;

        TMaybe<NYdb::TStatus> errStatus;

        auto checker = [&errStatus, this](NYdb::TAsyncStatus asyncStatus) mutable {
            auto status = asyncStatus.ExtractValue();
            if (!status.IsSuccess()) {
                FinishedByError = true;
                errStatus = status;
            }
            return NThreading::MakeFuture(status);
        };

        for (ui32 shard = 0; shard < Params_.ShardsCount; shard++) {
            auto uploadFn = CreateUploadFn(dataProvider, shard);
            resultFutures.push_back(RunUploadTask(dataProvider, shard, uploadFn).Apply(checker));
        }

        NThreading::WaitExceptionOrAll(resultFutures).Wait();
        if (errStatus)
            ThrowOnError(errStatus.GetRef());
    }

    TTableClient Client_;
    const TString Table_;
    const TUploaderParams Params_;
    const TString ParamName_ = "$items";
    TString Query_;
    bool FinishedByError = false;
    TVector<EUploadStatus> UploadStatuses_;
};

IUploader::TPtr CreateUploader(TDriver& driver, const TString& table, const TUploaderParams& params) {
    return std::make_unique<TUploader>(TTableClient(driver), table, params);
}

} // namespace NIdxTest

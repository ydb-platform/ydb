#include "defs.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx.h>

#include <util/generic/bitmap.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

namespace NKikimr {

using namespace NDataShard::NKqpHelpers;
using namespace Tests;

struct TProto {
    using TEvListingRequest = NKikimrTxDataShard::TEvObjectStorageListingRequest;
    using TEvListingResponse = NKikimrTxDataShard::TEvObjectStorageListingResponse;
};

namespace {

void CreateTable(TServer::TPtr server, const TActorId& sender, const TString& root, const TString& name) {
    auto opts = TShardedTableOptions()
        .Columns({
            {"bucket", "Uint32", true, false},
            {"path", "Utf8", true, false},
            {"value", "Utf8", false, false},
            {"deleted", "Bool", false, false}
        });
    CreateShardedTable(server, sender, root, name, opts);
}

TProto::TEvListingRequest MakeListingRequest(const TTableId& tableId, const std::optional<NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType> filterType) {
    TProto::TEvListingRequest request;

    TVector<TCell> bucketPrefixCell = {TCell::Make(100)};
    TString prefix = TSerializedCellVec::Serialize(bucketPrefixCell);

    request.SetTableId(tableId.PathId.LocalPathId);
    request.SetSerializedKeyPrefix(prefix);
    request.SetPathColumnPrefix("/test/");
    request.SetPathColumnDelimiter("/");
    request.SetMaxKeys(10);

    request.add_columnstoreturn(2);
    request.add_columnstoreturn(3);
    request.add_columnstoreturn(4);

    if (filterType) {
        TVector<TCell> filterCell = {TCell::Make(false)};
        auto* filter = request.mutable_filter();

        filter->add_columns(2);
        filter->set_values(TSerializedCellVec::Serialize(filterCell));
        filter->add_matchtypes(filterType.value());
    }

    return request;
}

template <typename TEvResponse, typename TDerived>
class TRequestRunner: public TActorBootstrapped<TDerived> {
    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status == NKikimrProto::OK) {
            return;
        }

        Bootstrap();
    }

protected:
    void Reply(typename TEvResponse::TPtr& ev) {
        TlsActivationContext->AsActorContext().Send(ev->Forward(ReplyTo));
    }

    virtual void Handle(typename TEvResponse::TPtr& ev) {
        Reply(ev);
        PassAway();
    }

    void PassAway() override {
        if (Pipe) {
            NTabletPipe::CloseAndForgetClient(this->SelfId(), Pipe);
        }

        IActor::PassAway();
    }

    virtual IEventBase* MakeRequest() const = 0;

public:
    explicit TRequestRunner(const TActorId& replyTo, ui64 tabletID)
        : ReplyTo(replyTo)
        , TabletID(tabletID)
    {
    }

    void Bootstrap() {
        if (Pipe) {
            NTabletPipe::CloseAndForgetClient(this->SelfId(), Pipe);
        }

        Pipe = this->Register(NTabletPipe::CreateClient(this->SelfId(), TabletID));
        NTabletPipe::SendData(this->SelfId(), Pipe, this->MakeRequest());

        this->Become(&TDerived::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            cFunc(TEvTabletPipe::TEvClientDestroyed::EventType, Bootstrap);
            hFunc(TEvResponse, Handle);
        }
    }

    using TBase = TRequestRunner<TEvResponse, TDerived>;

private:
    const TActorId ReplyTo;
    const ui64 TabletID;

    TActorId Pipe;
};

std::pair<std::vector<std::string>, std::vector<std::string>> List(
        TServer::TPtr server, const TActorId& sender, const TString& path,
        const std::optional<NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType> doFilter,
        ui32 status = NKikimrTxDataShard::TError::OK)
{
    using TEvRequest = TEvDataShard::TEvObjectStorageListingRequest;
    using TEvResponse = TEvDataShard::TEvObjectStorageListingResponse;

    class TLister: public TRequestRunner<TEvResponse, TLister> {
    public:
        explicit TLister(
                const TActorId& replyTo, ui64 tabletID,
                const TTableId& tableId, const std::optional<NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType> doFilter)
            : TBase(replyTo, tabletID)
            , TableId(tableId)
            , DoFilter(doFilter)
        {
        }

        IEventBase* MakeRequest() const override {
            auto request = MakeHolder<TEvRequest>();
            request->Record = MakeListingRequest(TableId, DoFilter);
            return request.Release();
        }

    private:
        const TTableId TableId;
        const std::optional<NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType> DoFilter;
    };

    auto tableId = ResolveTableId(server, sender, path);

    auto tabletIDs = GetTableShards(server, sender, path);
    UNIT_ASSERT_VALUES_EQUAL(tabletIDs.size(), 1);
    server->GetRuntime()->Register(new TLister(sender, tabletIDs[0], tableId, doFilter));

    auto ev = server->GetRuntime()->GrabEdgeEventRethrow<TEvResponse>(sender);

    auto& rec = ev->Get()->Record;

    UNIT_ASSERT_VALUES_EQUAL(static_cast<ui32>(rec.GetStatus()), status);

    std::vector<std::string> contents;
    std::vector<std::string> commonPrefixes;

    if (rec.CommonPrefixesRowsSize() > 0) {
        auto& folders = rec.commonprefixesrows();
        for (auto row : folders) {
            commonPrefixes.emplace_back(row);
        }
    }
        
    if (rec.ContentsRowsSize() > 0) {
        auto& files = rec.contentsrows();
        for (auto row : files) {
            auto vec = TSerializedCellVec(row);
            const auto& cell = vec.GetCells()[0];
            contents.emplace_back(cell.AsBuf().data(), cell.AsBuf().size());
        }
    }

    return std::make_pair(commonPrefixes, contents);
}

} // anonymous

Y_UNIT_TEST_SUITE(ObjectStorageListingTest) {

    Y_UNIT_TEST(ListingNoFilter) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (bucket, path, value, deleted) VALUES
            (100, "/test/foo.txt", "foo", false),
            (100, "/test/visible/bar.txt", "bar", false),
            (100, "/test/deleted/baz.txt", "baz", true),
            (100, "/test/foobar.txt", "foobar", false),
            (100, "/test/deleted.txt", "foobar", true)
        )");

        auto res = List(server, sender, "/Root/table-1", {});

        std::vector<std::string> expectedFolders = {"/test/deleted/", "/test/visible/"};
        std::vector<std::string> expectedFiles = {"/test/deleted.txt", "/test/foo.txt", "/test/foobar.txt"};

        UNIT_ASSERT_VALUES_EQUAL(expectedFolders, res.first);
        UNIT_ASSERT_VALUES_EQUAL(expectedFiles, res.second);
    }

    Y_UNIT_TEST(FilterListing) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings
            .SetDomainName("Root")
            .SetUseRealThreads(false);

        TServer::TPtr server = new TServer(serverSettings);
        auto& runtime = *server->GetRuntime();
        const TActorId sender = runtime.AllocateEdgeActor();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        InitRoot(server, sender);

        CreateTable(server, sender, "/Root", "table-1");
        ExecSQL(server, sender, R"(
            UPSERT INTO `/Root/table-1` (bucket, path, value, deleted) VALUES
            (100, "/test/foo.txt", "foo", false),
            (100, "/test/visible/bar.txt", "bar", false),
            (100, "/test/deleted/baz.txt", "baz", true),
            (100, "/test/foobar.txt", "foobar", false),
            (100, "/test/deleted.txt", "foobar", true)
        )");

        {
            auto res = List(server, sender, "/Root/table-1", NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_EQUAL);

            std::vector<std::string> expectedFolders = {"/test/visible/"};
            std::vector<std::string> expectedFiles = {"/test/foo.txt", "/test/foobar.txt"};

            UNIT_ASSERT_VALUES_EQUAL(expectedFolders, res.first);
            UNIT_ASSERT_VALUES_EQUAL(expectedFiles, res.second);
        }

        {
            auto res = List(server, sender, "/Root/table-1", NKikimrTxDataShard::TObjectStorageListingFilter_EMatchType_NOT_EQUAL);

            std::vector<std::string> expectedFolders = {"/test/deleted/"};
            std::vector<std::string> expectedFiles = {"/test/deleted.txt"};

            UNIT_ASSERT_VALUES_EQUAL(expectedFolders, res.first);
            UNIT_ASSERT_VALUES_EQUAL(expectedFiles, res.second);
        }
    }
}

} // namespace NKikimr

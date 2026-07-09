#pragma once

#include "client.h"

#include <util/generic/hash.h>
#include <util/system/mutex.h>
#include <yt/cpp/mapreduce/interface/fwd.h>

namespace NYql::NConnector::NApi {
    class TSchema;
} // namespace NYql::NConnector::NApi

namespace NYql::NConnector {

    // TYtClient is a YT-native implementation of the generic connector IClient interface.
    // Instead of talking to fq-connector-go over gRPC, it accesses YTsaurus directly via
    // the YT C++ client library (yt/cpp/mapreduce). It reuses all the generic-provider
    // machinery (read actor, lookup actor for map joins, Dq integration) by producing the
    // very same request/response protobufs that the gRPC client would.
    //
    // Read path:
    //   * DescribeTable maps the YT table @schema to the connector TSchema.
    //   * ListSplits maps the table into TSplit entries. It honors max_split_count == 1
    //     (the lookup-join path) by returning exactly one split covering the whole table.
    //   * ReadSplits reads YT rows for the requested splits and produces Arrow IPC blocks.
    //     It applies the TSelect.where.filter_typed predicate under FILTERING_MANDATORY
    //     (map-join point lookups: a disjunction of per-key EQ conjunctions) and the
    //     TSelect.limit under FILTERING_OPTIONAL (fullscan-with-limit lookups).
    //
    // Write path (used later by the generic sink actor):
    //   * WriteRows appends the given rows to a YT table according to the supplied schema.
    class TYtClient: public IClient {
    public:
        explicit TYtClient(const TGenericGatewayConfig& config);
        ~TYtClient() override = default;

        TDescribeTableAsyncResult DescribeTable(const NApi::TDescribeTableRequest& request,
                                                TDuration timeout = {}) override;
        TListSplitsStreamIteratorAsyncResult ListSplits(const NApi::TListSplitsRequest& request,
                                                        TDuration timeout = {}) override;
        TReadSplitsStreamIteratorAsyncResult ReadSplits(const NApi::TReadSplitsRequest& request,
                                                        TDuration timeout = {}) override;

        // Write entrypoint used by the generic sink actor (implemented in Stage 12).
        // Appends the rows contained in the serialized Arrow IPC block to the table
        // described by the data source instance, using the given row schema.
        void WriteRows(const NApi::TSchema& schema,
                       const TString& table,
                       const TString& arrowIpcStreaming,
                       const NYql::TGenericDataSourceInstance& dataSourceInstance) override;

    protected:
        // Obtains (creating and caching if necessary) a YT client for the cluster and
        // token described by the given data source instance. Virtual so that unit tests
        // can inject an in-memory / local YT client instead of connecting to a real
        // cluster.
        virtual NYT::IClientPtr GetYtClient(const NYql::TGenericDataSourceInstance& dataSourceInstance);

        // Data-access seams. These wrap the few YT operations the client depends on so
        // that unit tests can supply in-memory fixtures instead of talking to a live
        // (or recipe-launched local) YT cluster.
        //
        // GetNode fetches a Cypress node (used for "<table>/@schema" and
        // "<table>/@row_count"). ReadRows reads the rows selected by the given rich path
        // (already carrying the row-range and column projection).
        virtual NYT::TNode GetNode(const NYT::IClientPtr& client, const TString& path);
        virtual TVector<NYT::TNode> ReadRows(const NYT::IClientPtr& client, const NYT::TRichYPath& path);

        static TString GetClusterName(const NYql::TGenericDataSourceInstance& dataSourceInstance);
        static TString GetToken(const NYql::TGenericDataSourceInstance& dataSourceInstance);

    private:
        const TGenericGatewayConfig Config_;

        TMutex Mutex_;
        // key = cluster name + '\0' + token
        THashMap<TString, NYT::IClientPtr> ClientForCluster_;
    };

    IClient::TPtr MakeYtClient(const ::NYql::TGenericGatewayConfig& cfg);

} // namespace NYql::NConnector

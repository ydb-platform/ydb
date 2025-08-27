#!/bin/bash

export YDB_DATABASE=/Root
export YDB_ENDPOINT=localhost:12345

#ya make -AP -ttt -F*StreamingQueryWithYdbJoin*
./ydb-core-kqp-ut-federated_query-datastreams -F KqpStreamingQueriesDdl::StreamingQueryWithYdbJoin
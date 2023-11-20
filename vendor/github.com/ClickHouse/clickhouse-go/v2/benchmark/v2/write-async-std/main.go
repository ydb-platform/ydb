// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

const ddl = `
CREATE TABLE benchmark_async (
	  Col1 UInt64
	, Col2 String
	, Col3 Array(UInt8)
	, Col4 DateTime
) Engine Null
`

func benchmark(conn *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = clickhouse.Context(ctx, clickhouse.WithStdAsync(false))
	{
		for i := 0; i < 10_000; i++ {
			_, err := conn.ExecContext(ctx, fmt.Sprintf(`INSERT INTO benchmark_async VALUES (
				%d, '%s', [1, 2, 3, 4, 5, 6, 7, 8, 9], now()
			)`, i, "Golang SQL database driver"))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func main() {
	conn, err := sql.Open("clickhouse", "clickhouse://127.0.0.1:9000")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec("DROP TABLE IF EXISTS benchmark_async"); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ddl); err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	if err := benchmark(conn); err != nil {
		log.Fatal(err)
	}
	fmt.Println(time.Since(start))
}

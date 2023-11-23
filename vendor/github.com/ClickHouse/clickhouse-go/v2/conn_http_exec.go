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

package clickhouse

import (
	"context"
	"io"
	"io/ioutil"
	"strings"
)

func (h *httpConnect) exec(ctx context.Context, query string, args ...interface{}) error {
	query, err := bind(h.location, query, args...)
	if err != nil {
		return err
	}

	options := queryOptions(ctx)

	res, err := h.sendQuery(ctx, strings.NewReader(query), &options, h.headers)
	if res != nil {
		defer res.Body.Close()
		// we don't care about result, so just discard it to reuse connection
		_, _ = io.Copy(ioutil.Discard, res.Body)
	}

	return err
}

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

package cityhash102

import (
	"encoding/binary"
	"hash"
)

type City64 struct {
	s []byte
}

var _ hash.Hash64 = (*City64)(nil)
var _ hash.Hash = (*City64)(nil)

func New64() hash.Hash64 {
	return &City64{}
}

func (this *City64) Sum(b []byte) []byte {
	b2 := make([]byte, 8)
	binary.BigEndian.PutUint64(b2, this.Sum64())
	b = append(b, b2...)
	return b
}

func (this *City64) Sum64() uint64 {
	return CityHash64(this.s, uint32(len(this.s)))
}

func (this *City64) Reset() {
	this.s = this.s[0:0]
}

func (this *City64) BlockSize() int {
	return 1
}

func (this *City64) Write(s []byte) (n int, err error) {
	this.s = append(this.s, s...)
	return len(s), nil
}

func (this *City64) Size() int {
	return 8
}

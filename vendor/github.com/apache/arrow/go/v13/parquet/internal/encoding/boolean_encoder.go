// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package encoding

import (
	"github.com/apache/arrow/go/v13/arrow/bitutil"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/internal/utils"
)

const (
	boolBufSize = 1024
	boolsInBuf  = boolBufSize * 8
)

// PlainBooleanEncoder encodes bools as a bitmap as per the Plain Encoding
type PlainBooleanEncoder struct {
	encoder
	bitsBuffer []byte
	wr         utils.BitmapWriter
}

// Type for the PlainBooleanEncoder is parquet.Types.Boolean
func (PlainBooleanEncoder) Type() parquet.Type {
	return parquet.Types.Boolean
}

// Put encodes the contents of in into the underlying data buffer.
func (enc *PlainBooleanEncoder) Put(in []bool) {
	if enc.bitsBuffer == nil {
		enc.bitsBuffer = make([]byte, boolBufSize)
	}
	if enc.wr == nil {
		enc.wr = utils.NewBitmapWriter(enc.bitsBuffer, 0, boolsInBuf)
	}
	if len(in) == 0 {
		return
	}

	n := enc.wr.AppendBools(in)
	for n < len(in) {
		enc.wr.Finish()
		enc.append(enc.bitsBuffer)
		enc.wr.Reset(0, boolsInBuf)
		in = in[n:]
		n = enc.wr.AppendBools(in)
	}
}

// PutSpaced will use the validBits bitmap to determine which values are nulls
// and can be left out from the slice, and the encoded without those nulls.
func (enc *PlainBooleanEncoder) PutSpaced(in []bool, validBits []byte, validBitsOffset int64) {
	bufferOut := make([]bool, len(in))
	nvalid := spacedCompress(in, bufferOut, validBits, validBitsOffset)
	enc.Put(bufferOut[:nvalid])
}

// EstimatedDataEncodedSize returns the current number of bytes that have
// been buffered so far
func (enc *PlainBooleanEncoder) EstimatedDataEncodedSize() int64 {
	return int64(enc.sink.Len() + int(bitutil.BytesForBits(int64(enc.wr.Pos()))))
}

// FlushValues returns the buffered data, the responsibility is on the caller
// to release the buffer memory
func (enc *PlainBooleanEncoder) FlushValues() (Buffer, error) {
	if enc.wr.Pos() > 0 {
		toFlush := int(enc.wr.Pos())
		enc.append(enc.bitsBuffer[:bitutil.BytesForBits(int64(toFlush))])
	}

	enc.wr.Reset(0, boolsInBuf)

	return enc.sink.Finish(), nil
}

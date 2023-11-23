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

package pqarrow

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/internal/utils"
	"github.com/apache/arrow/go/v13/parquet"
	"github.com/apache/arrow/go/v13/parquet/file"
	"github.com/apache/arrow/go/v13/parquet/metadata"
	"golang.org/x/xerrors"
)

// WriteTable is a convenience function to create and write a full array.Table to a parquet file. The schema
// and columns will be determined by the schema of the table, writing the file out to the provided writer.
// The chunksize will be utilized in order to determine the size of the row groups.
func WriteTable(tbl arrow.Table, w io.Writer, chunkSize int64, props *parquet.WriterProperties, arrprops ArrowWriterProperties) error {
	writer, err := NewFileWriter(tbl.Schema(), w, props, arrprops)
	if err != nil {
		return err
	}

	if err := writer.WriteTable(tbl, chunkSize); err != nil {
		return err
	}

	return writer.Close()
}

// FileWriter is an object for writing Arrow directly to a parquet file.
type FileWriter struct {
	wr         *file.Writer
	schema     *arrow.Schema
	manifest   *SchemaManifest
	rgw        file.RowGroupWriter
	arrowProps ArrowWriterProperties
	ctx        context.Context
	colIdx     int
	closed     bool
}

// NewFileWriter returns a writer for writing Arrow directly to a parquetfile, rather than
// the ArrowColumnWriter and WriteArrow functions which allow writing arrow to an existing
// file.Writer, this will create a new file.Writer based on the schema provided.
func NewFileWriter(arrschema *arrow.Schema, w io.Writer, props *parquet.WriterProperties, arrprops ArrowWriterProperties) (*FileWriter, error) {
	if props == nil {
		props = parquet.NewWriterProperties()
	}

	pqschema, err := ToParquet(arrschema, props, arrprops)
	if err != nil {
		return nil, err
	}

	meta := make(metadata.KeyValueMetadata, 0)
	for i := 0; i < arrschema.Metadata().Len(); i++ {
		meta.Append(arrschema.Metadata().Keys()[i], arrschema.Metadata().Values()[i])
	}

	if arrprops.storeSchema {
		serializedSchema := flight.SerializeSchema(arrschema, props.Allocator())
		meta.Append("ARROW:schema", base64.StdEncoding.EncodeToString(serializedSchema))
	}

	schemaNode := pqschema.Root()
	baseWriter := file.NewParquetWriter(w, schemaNode, file.WithWriterProps(props), file.WithWriteMetadata(meta))

	manifest, err := NewSchemaManifest(pqschema, nil, &ArrowReadProperties{})
	if err != nil {
		return nil, err
	}

	return &FileWriter{wr: baseWriter, schema: arrschema, manifest: manifest, arrowProps: arrprops, ctx: NewArrowWriteContext(context.TODO(), &arrprops)}, nil
}

// NewRowGroup does what it says on the tin, creates a new row group in the underlying file.
// Equivalent to `AppendRowGroup` on a file.Writer
func (fw *FileWriter) NewRowGroup() {
	if fw.rgw != nil {
		fw.rgw.Close()
	}
	fw.rgw = fw.wr.AppendRowGroup()
	fw.colIdx = 0
}

// NewBufferedRowGroup starts a new memory Buffered Row Group to allow writing columns / records
// without immediately flushing them to disk. This allows using WriteBuffered to write records
// and decide where to break your rowgroup based on the TotalBytesWritten rather than on the max
// row group len. If using Records, this should be paired with WriteBuffered, while
// Write will always write a new record as a row group in and of itself.
func (fw *FileWriter) NewBufferedRowGroup() {
	if fw.rgw != nil {
		fw.rgw.Close()
	}
	fw.rgw = fw.wr.AppendBufferedRowGroup()
	fw.colIdx = 0
}

// RowGroupTotalCompressedBytes returns the total number of bytes after compression
// that have been written to the current row group so far.
func (fw *FileWriter) RowGroupTotalCompressedBytes() int64 {
	if fw.rgw != nil {
		return fw.rgw.TotalCompressedBytes()
	}
	return 0
}

// RowGroupTotalBytesWritten returns the total number of bytes written and flushed out in
// the current row group.
func (fw *FileWriter) RowGroupTotalBytesWritten() int64 {
	if fw.rgw != nil {
		return fw.rgw.TotalBytesWritten()
	}
	return 0
}

func (fw *FileWriter) WriteBuffered(rec arrow.Record) error {
	if !rec.Schema().Equal(fw.schema) {
		return fmt.Errorf("record schema does not match writer's. \nrecord: %s\nwriter: %s", rec.Schema(), fw.schema)
	}

	var (
		recList []arrow.Record
		maxRows = fw.wr.Properties().MaxRowGroupLength()
		curRows int
		err     error
	)
	if fw.rgw != nil {
		if curRows, err = fw.rgw.NumRows(); err != nil {
			return err
		}
	} else {
		fw.NewBufferedRowGroup()
	}

	if int64(curRows)+rec.NumRows() <= maxRows {
		recList = []arrow.Record{rec}
	} else {
		recList = []arrow.Record{rec.NewSlice(0, maxRows-int64(curRows))}
		defer recList[0].Release()
		for offset := maxRows - int64(curRows); offset < rec.NumRows(); offset += maxRows {
			s := rec.NewSlice(offset, offset+utils.Min(maxRows, rec.NumRows()-offset))
			defer s.Release()
			recList = append(recList, s)
		}
	}

	for idx, r := range recList {
		if idx > 0 {
			fw.NewBufferedRowGroup()
		}
		for i := 0; i < int(r.NumCols()); i++ {
			if err := fw.WriteColumnData(r.Column(i)); err != nil {
				fw.Close()
				return err
			}
		}
	}
	fw.colIdx = 0
	return nil
}

// Write an arrow Record Batch to the file, respecting the MaxRowGroupLength in the writer
// properties to determine whether or not a new row group is created while writing.
func (fw *FileWriter) Write(rec arrow.Record) error {
	if !rec.Schema().Equal(fw.schema) {
		return fmt.Errorf("record schema does not match writer's. \nrecord: %s\nwriter: %s", rec.Schema(), fw.schema)
	}

	var recList []arrow.Record
	rowgroupLen := fw.wr.Properties().MaxRowGroupLength()
	if rec.NumRows() > rowgroupLen {
		recList = make([]arrow.Record, 0)
		for offset := int64(0); offset < rec.NumRows(); offset += rowgroupLen {
			s := rec.NewSlice(offset, offset+utils.Min(rowgroupLen, rec.NumRows()-offset))
			defer s.Release()
			recList = append(recList, s)
		}
	} else {
		recList = []arrow.Record{rec}
	}

	for _, r := range recList {
		fw.NewRowGroup()
		for i := 0; i < int(r.NumCols()); i++ {
			if err := fw.WriteColumnData(r.Column(i)); err != nil {
				fw.Close()
				return err
			}
		}
	}
	fw.colIdx = 0
	return nil
}

// WriteTable writes an arrow table to the underlying file using chunkSize to determine
// the size to break at for making row groups. Writing a table will always create a new
// row group for each chunk of chunkSize rows in the table. Calling this with 0 rows will
// still write a 0 length Row Group to the file.
func (fw *FileWriter) WriteTable(tbl arrow.Table, chunkSize int64) error {
	if chunkSize <= 0 && tbl.NumRows() > 0 {
		return xerrors.New("chunk size per row group must be greater than 0")
	} else if !tbl.Schema().Equal(fw.schema) {
		return fmt.Errorf("table schema does not match writer's. \nTable: %s\n writer: %s", tbl.Schema(), fw.schema)
	} else if chunkSize > fw.wr.Properties().MaxRowGroupLength() {
		chunkSize = fw.wr.Properties().MaxRowGroupLength()
	}

	writeRowGroup := func(offset, size int64) error {
		fw.NewRowGroup()
		for i := 0; i < int(tbl.NumCols()); i++ {
			if err := fw.WriteColumnChunked(tbl.Column(i).Data(), offset, size); err != nil {
				return err
			}
		}
		return nil
	}

	if tbl.NumRows() == 0 {
		if err := writeRowGroup(0, 0); err != nil {
			fw.Close()
			return err
		}
		return nil
	}

	for offset := int64(0); offset < tbl.NumRows(); offset += chunkSize {
		if err := writeRowGroup(offset, utils.Min(chunkSize, tbl.NumRows()-offset)); err != nil {
			fw.Close()
			return err
		}
	}
	return nil
}

// Close flushes out the data and closes the file. It can be called multiple times,
// subsequent calls after the first will have no effect.
func (fw *FileWriter) Close() error {
	if !fw.closed {
		fw.closed = true
		if fw.rgw != nil {
			if err := fw.rgw.Close(); err != nil {
				return err
			}
		}

		writeCtx := arrowCtxFromContext(fw.ctx)
		if writeCtx.dataBuffer != nil {
			writeCtx.dataBuffer.Release()
			writeCtx.dataBuffer = nil
		}

		return fw.wr.Close()
	}
	return nil
}

// WriteColumnChunked will write the data provided to the underlying file, using the provided
// offset and size to allow writing subsets of data from the chunked column. It uses the current
// column in the underlying row group writer as the starting point, allowing progressive
// building of writing columns to a file via arrow data without needing to already have
// a record or table.
func (fw *FileWriter) WriteColumnChunked(data *arrow.Chunked, offset, size int64) error {
	acw, err := NewArrowColumnWriter(data, offset, size, fw.manifest, fw.rgw, fw.colIdx)
	if err != nil {
		return err
	}
	fw.colIdx += acw.leafCount
	return acw.Write(fw.ctx)
}

// WriteColumnData writes the entire array to the file as the next columns. Like WriteColumnChunked
// it is based on the current column of the row group writer allowing progressive building
// of the file by columns without needing a full record or table to write.
func (fw *FileWriter) WriteColumnData(data arrow.Array) error {
	chunked := arrow.NewChunked(data.DataType(), []arrow.Array{data})
	defer chunked.Release()
	return fw.WriteColumnChunked(chunked, 0, int64(data.Len()))
}

package proto

import (
	"github.com/google/uuid"
)

// ColUUID is UUID column.
type ColUUID []uuid.UUID

// Compile-time assertions for ColUUID.
var (
	_ ColInput            = ColUUID{}
	_ ColResult           = (*ColUUID)(nil)
	_ Column              = (*ColUUID)(nil)
	_ ColumnOf[uuid.UUID] = (*ColUUID)(nil)
)

func (c ColUUID) Type() ColumnType         { return ColumnTypeUUID }
func (c ColUUID) Rows() int                { return len(c) }
func (c ColUUID) Row(i int) uuid.UUID      { return c[i] }
func (c *ColUUID) Reset()                  { *c = (*c)[:0] }
func (c *ColUUID) Append(v uuid.UUID)      { *c = append(*c, v) }
func (c *ColUUID) AppendArr(v []uuid.UUID) { *c = append(*c, v...) }

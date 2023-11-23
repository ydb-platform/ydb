package proto

import (
	"github.com/go-faster/errors"
)

// Compile-time assertions for ColMap.
var (
	_ ColInput                 = (*ColMap[string, string])(nil)
	_ ColResult                = (*ColMap[string, string])(nil)
	_ Column                   = (*ColMap[string, string])(nil)
	_ ColumnOf[map[string]int] = (*ColMap[string, int])(nil)
	_ StateEncoder             = (*ColMap[string, string])(nil)
	_ StateDecoder             = (*ColMap[string, string])(nil)

	_ = ColMap[int64, string]{
		Keys:   new(ColInt64),
		Values: new(ColStr),
	}
)

// NewMap constructs Map(K, V).
func NewMap[K comparable, V any](k ColumnOf[K], v ColumnOf[V]) *ColMap[K, V] {
	return &ColMap[K, V]{
		Keys:   k,
		Values: v,
	}
}

// ColMap implements Map(K, V) as ColumnOf[map[K]V].
type ColMap[K comparable, V any] struct {
	Offsets ColUInt64
	Keys    ColumnOf[K]
	Values  ColumnOf[V]
}

func (c ColMap[K, V]) Type() ColumnType {
	return ColumnTypeMap.Sub(c.Keys.Type(), c.Values.Type())
}

func (c ColMap[K, V]) Rows() int {
	return c.Offsets.Rows()
}

func (c *ColMap[K, V]) DecodeState(r *Reader) error {
	if s, ok := c.Keys.(StateDecoder); ok {
		if err := s.DecodeState(r); err != nil {
			return errors.Wrap(err, "keys state")
		}
	}
	if s, ok := c.Values.(StateDecoder); ok {
		if err := s.DecodeState(r); err != nil {
			return errors.Wrap(err, "values state")
		}
	}
	return nil
}

func (c ColMap[K, V]) EncodeState(b *Buffer) {
	if s, ok := c.Keys.(StateEncoder); ok {
		s.EncodeState(b)
	}
	if s, ok := c.Values.(StateEncoder); ok {
		s.EncodeState(b)
	}
}

func (c ColMap[K, V]) Row(i int) map[K]V {
	m := make(map[K]V)
	var start int
	end := int(c.Offsets[i])
	if i > 0 {
		start = int(c.Offsets[i-1])
	}
	for idx := start; idx < end; idx++ {
		m[c.Keys.Row(idx)] = c.Values.Row(idx)
	}
	return m
}

func (c *ColMap[K, V]) Append(m map[K]V) {
	for k, v := range m {
		c.Keys.Append(k)
		c.Values.Append(v)
	}
	c.Offsets.Append(uint64(c.Keys.Rows()))
}

func (c *ColMap[K, V]) AppendArr(v []map[K]V) {
	for _, m := range v {
		c.Append(m)
	}
}

func (c *ColMap[K, V]) DecodeColumn(r *Reader, rows int) error {
	if rows == 0 {
		return nil
	}
	if err := c.Offsets.DecodeColumn(r, rows); err != nil {
		return errors.Wrap(err, "offsets")
	}

	count := int(c.Offsets[rows-1])
	if err := checkRows(count); err != nil {
		return errors.Wrap(err, "keys count")
	}
	if err := c.Keys.DecodeColumn(r, count); err != nil {
		return errors.Wrap(err, "keys")
	}
	if err := c.Values.DecodeColumn(r, count); err != nil {
		return errors.Wrap(err, "values")
	}

	return nil
}

func (c *ColMap[K, V]) Reset() {
	c.Offsets.Reset()
	c.Keys.Reset()
	c.Values.Reset()
}

func (c ColMap[K, V]) EncodeColumn(b *Buffer) {
	if c.Rows() == 0 {
		return
	}

	c.Offsets.EncodeColumn(b)
	c.Keys.EncodeColumn(b)
	c.Values.EncodeColumn(b)
}

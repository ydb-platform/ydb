package paging

import (
	"fmt"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
)

// Some acceptors belong to fixed-size types (integers, floats, booleans),
// while the others contain variable length types (arrays, slices, maps, strings).
// We will save CPU if we calculate the size of acceptors of fixed types only once.
type acceptorKind int8

const (
	unknownSize acceptorKind = iota
	fixedSize
	variableSize
)

type sizePattern[T utils.Acceptor] struct {
	// Ordered numbers of acceptors of variable legnth types.
	// Their size must be estimated every time.
	varyingSizeIx []int
	// We summarize the size of fixed size acceptors here.
	// They never change between the rows.
	fixedSizeTotal uint64
}

func (sp *sizePattern[T]) estimate(acceptors []T) (uint64, error) {
	sizeTotal := sp.fixedSizeTotal

	for _, ix := range sp.varyingSizeIx {
		sizeVariable, _, err := sizeOfValue(acceptors[ix])
		if err != nil {
			return 0, fmt.Errorf("size of value #%d: %w", ix, err)
		}

		sizeTotal += sizeVariable
	}

	return sizeTotal, nil
}

func newSizePattern[T utils.Acceptor](acceptors []T) (*sizePattern[T], error) {
	sp := &sizePattern[T]{}

	for i, acceptor := range acceptors {
		size, kind, err := sizeOfValue(acceptor)
		if err != nil {
			return nil, fmt.Errorf("estimate size of value #%d: %w", i, err)
		}

		switch kind {
		case fixedSize:
			sp.fixedSizeTotal += size
		case variableSize:
			sp.varyingSizeIx = append(sp.varyingSizeIx, i)
		default:
			return nil, fmt.Errorf("unknown type kind: %w", err)
		}
	}

	return sp, nil
}

func sizeOfValue(v any) (uint64, acceptorKind, error) {
	reflected := reflect.ValueOf(v)

	// for nil values
	value := reflect.Indirect(reflected)

	if value.Kind() == reflect.Ptr {
		// unwrap double pointer
		value = reflect.Indirect(value)
	}

	if !value.IsValid() {
		return 0, variableSize, nil
	}

	// TODO: in order to support complicated and composite data types
	// one should write reflection code in spite of
	// https://github.com/DmitriyVTitov/size/blob/master/size.go
	switch t := value.Interface().(type) {
	case bool:
		return 1, fixedSize, nil
	case int8, uint8:
		return 1, fixedSize, nil
	case int16, uint16:
		return 2, fixedSize, nil
	case int32, uint32, float32:
		return 4, fixedSize, nil
	case int64, uint64, float64:
		return 8, fixedSize, nil
	case time.Time:
		// time.Time and all its derivatives consist of two 8-byte ints:
		// https://cs.opensource.google/go/go/+/refs/tags/go1.21.4:src/time/time.go;l=141-142
		// Location is ignored.
		return 16, fixedSize, nil
	case []byte:
		return uint64(len(t)), variableSize, nil
	case string:
		return uint64(len(t)), variableSize, nil
	case pgtype.Bool:
		return 1, fixedSize, nil
	case pgtype.Int2:
		return 2, fixedSize, nil
	case pgtype.Int4:
		return 4, fixedSize, nil
	case pgtype.Int8:
		return 8, fixedSize, nil
	case pgtype.Float4:
		return 4, fixedSize, nil
	case pgtype.Float8:
		return 8, fixedSize, nil
	case pgtype.Text:
		return uint64(len(t.String)), variableSize, nil
	case pgtype.Date:
		return 16, fixedSize, nil
	case pgtype.Timestamp:
		return 16, fixedSize, nil
	default:
		return 0, 0, fmt.Errorf("value %v of unexpected data type %T: %w", t, t, utils.ErrDataTypeNotSupported)
	}
}

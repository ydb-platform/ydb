package utils

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

func SelectWhatToYDBTypes(selectWhat *api_service_protos.TSelect_TWhat) ([]*Ydb.Type, error) {
	var ydbTypes []*Ydb.Type

	for i, item := range selectWhat.Items {
		ydbType := item.GetColumn().GetType()
		if ydbType == nil {
			return nil, fmt.Errorf("item #%d (%v) is not a column", i, item)
		}

		ydbTypes = append(ydbTypes, ydbType)
	}

	return ydbTypes, nil
}

func YdbTypeToYdbPrimitiveTypeID(ydbType *Ydb.Type) (Ydb.Type_PrimitiveTypeId, error) {
	switch t := ydbType.Type.(type) {
	case *Ydb.Type_TypeId:
		return t.TypeId, nil
	case *Ydb.Type_OptionalType:
		switch t.OptionalType.Item.Type.(type) {
		case *Ydb.Type_TypeId:
			return t.OptionalType.Item.GetTypeId(), nil
		default:
			return Ydb.Type_PRIMITIVE_TYPE_ID_UNSPECIFIED, fmt.Errorf("unexpected type %v: %w", t.OptionalType.Item, ErrDataTypeNotSupported)
		}
	default:
		return Ydb.Type_PRIMITIVE_TYPE_ID_UNSPECIFIED, fmt.Errorf("unexpected type %v: %w", t, ErrDataTypeNotSupported)
	}
}

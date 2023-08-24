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

func SelectWhatToYDBColumns(selectWhat *api_service_protos.TSelect_TWhat) ([]*Ydb.Column, error) {
	var columns []*Ydb.Column

	for i, item := range selectWhat.Items {
		column := item.GetColumn()
		if column == nil {
			return nil, fmt.Errorf("item #%d (%v) is not a column", i, item)
		}

		columns = append(columns, column)
	}

	return columns, nil
}

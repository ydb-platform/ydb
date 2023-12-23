package s3

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"

	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/datasource"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/paging"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server/utils"
	api_service_protos "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/libgo/service/protos"
)

var _ datasource.DataSource[string] = (*dataSource)(nil)

type dataSource struct {
}

func (ds *dataSource) DescribeTable(ctx context.Context, _ log.Logger, request *api_service_protos.TDescribeTableRequest) (*api_service_protos.TDescribeTableResponse, error) {
	return nil, fmt.Errorf("table description is not implemented for schemaless data sources: %w", utils.ErrMethodNotSupported)
}

func (ds *dataSource) ReadSplit(ctx context.Context, logger log.Logger, split *api_service_protos.TSplit, sink paging.Sink[string]) {
	if err := ds.doReadSplit(ctx, logger, split, sink); err != nil {
		sink.AddError(err)
	}

	sink.Finish()
}

func (ds *dataSource) doReadSplit(
	ctx context.Context,
	logger log.Logger,
	split *api_service_protos.TSplit,
	sink paging.Sink[string]) error {
	conn := makeConnection()

	var (
		bucket string
		key    string
	)

	if bucket = split.Select.DataSourceInstance.GetS3Options().GetBucket(); bucket == "" {
		return fmt.Errorf("empty field `bucket`: %w", utils.ErrInvalidRequest)
	}

	if key = split.Select.From.GetObjectKey(); key == "" {
		return fmt.Errorf("empty field `key`: %w", utils.ErrInvalidRequest)
	}

	params := &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}

	response, err := conn.GetObject(ctx, params)
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}

	defer response.Body.Close()

	csvReader := csv.NewReader(response.Body)

	if err := transformCSV(split.Select.What, split.Select.PredefinedSchema, csvReader, sink); err != nil {
		return fmt.Errorf("transform csv: %w", err)
	}

	return nil
}

func makeAppender(ydbType *Ydb.Type) (func(acceptor string, builder array.Builder) error, error) {
	var appender func(acceptor string, builder array.Builder) error

	typeID := ydbType.GetTypeId()
	switch typeID {
	case Ydb.Type_INT32:
		appender = func(acceptor string, builder array.Builder) error {
			value, err := strconv.Atoi(acceptor)
			if err != nil {
				return fmt.Errorf("strconv atoi '%v': %w", acceptor, err)
			}

			builder.(*array.Int32Builder).Append(int32(value))

			return nil
		}
	case Ydb.Type_STRING:
		appender = func(acceptor string, builder array.Builder) error {
			builder.(*array.StringBuilder).Append(acceptor)

			return nil
		}
	default:
		return nil, fmt.Errorf("unexpected type %v: %w", typeID, utils.ErrDataTypeNotSupported)
	}

	return appender, nil
}

func prepareReading(
	selectWhat *api_service_protos.TSelect_TWhat,
	schema *api_service_protos.TSchema,
) ([]int, []func(acceptor string, builder array.Builder) error, error) {
	result := make([]int, 0, len(selectWhat.Items))
	appenders := make([]func(acceptor string, builder array.Builder) error, 0, len(selectWhat.Items))

	for _, item := range selectWhat.Items {
		for i, column := range schema.Columns {
			if item.GetColumn().Name == column.Name {
				result = append(result, i)

				appender, err := makeAppender(column.Type)
				if err != nil {
					return nil, nil, fmt.Errorf("make appender for column #%d: %w", i, err)
				}

				appenders = append(appenders, appender)
			}
		}
	}

	if len(result) != len(selectWhat.Items) {
		return nil, nil, fmt.Errorf(
			"requested column with schema mismatch (wanted %d columns, found only %d): %w",
			len(selectWhat.Items), len(result), utils.ErrInvalidRequest,
		)
	}

	return result, appenders, nil
}

func transformCSV(
	selectWhat *api_service_protos.TSelect_TWhat,
	schema *api_service_protos.TSchema,
	csvReader *csv.Reader,
	sink paging.Sink[string],
) error {
	wantedColumnIds, appenders, err := prepareReading(selectWhat, schema)
	if err != nil {
		return fmt.Errorf("get wanted columns ids: %w", err)
	}

	transformer := utils.NewRowTransformer[string](nil, appenders, wantedColumnIds)

	for {
		row, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}

			return fmt.Errorf("csv reader failure: %w", err)
		}

		if len(schema.Columns) != len(row) {
			return fmt.Errorf("schema and data mismatch: expected %d columns, got %d", len(schema.Columns), len(row))
		}

		// Save the row that was just read to make data accessible for other pipeline stages
		transformer.SetAcceptors(row)
	}

	return nil
}

func makeConnection() *s3.Client {
	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:       "aws",
			URL:               "http://127.0.0.1:9000",
			SigningRegion:     "us-east-2",
			HostnameImmutable: true,
		}, nil
	})

	conn := s3.NewFromConfig(aws.Config{
		Region:                      "us-east-2",
		Credentials:                 credentials.NewStaticCredentialsProvider("admin", "password", ""),
		EndpointResolverWithOptions: resolver,
	}, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	return conn
}

func NewDataSource() datasource.DataSource[string] {
	return &dataSource{}
}

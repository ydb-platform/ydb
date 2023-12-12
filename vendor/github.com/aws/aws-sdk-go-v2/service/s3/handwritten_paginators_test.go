package s3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"testing"
)

type mockListObjectVersionsClient struct {
	outputs []*ListObjectVersionsOutput
	inputs  []*ListObjectVersionsInput
	t       *testing.T
}

type mockListMultipartUploadsClient struct {
	outputs []*ListMultipartUploadsOutput
	inputs  []*ListMultipartUploadsInput
	t       *testing.T
}

func (c *mockListObjectVersionsClient) ListObjectVersions(ctx context.Context, input *ListObjectVersionsInput, optFns ...func(*Options)) (*ListObjectVersionsOutput, error) {
	c.inputs = append(c.inputs, input)
	requestCnt := len(c.inputs)
	testCurRequest(len(c.outputs), requestCnt, c.outputs[requestCnt-1].MaxKeys, input.MaxKeys, c.t)
	return c.outputs[requestCnt-1], nil
}

func (c *mockListMultipartUploadsClient) ListMultipartUploads(ctx context.Context, input *ListMultipartUploadsInput, optFns ...func(*Options)) (*ListMultipartUploadsOutput, error) {
	c.inputs = append(c.inputs, input)
	requestCnt := len(c.inputs)
	testCurRequest(len(c.outputs), requestCnt, c.outputs[requestCnt-1].MaxUploads, input.MaxUploads, c.t)
	return c.outputs[requestCnt-1], nil
}

type testCase struct {
	bucket                 *string
	limit                  int32
	requestCnt             int
	stopOnDuplicationToken bool
}

type listOVTestCase struct {
	testCase
	outputs []*ListObjectVersionsOutput
}

type listMPUTestCase struct {
	testCase
	outputs []*ListMultipartUploadsOutput
}

func TestListObjectVersionsPaginator(t *testing.T) {
	cases := map[string]listOVTestCase{
		"page limit 5": {
			testCase: testCase{
				bucket:     aws.String("testBucket1"),
				limit:      5,
				requestCnt: 3,
			},
			outputs: []*ListObjectVersionsOutput{
				{
					NextKeyMarker:       aws.String("testKey1"),
					NextVersionIdMarker: aws.String("testID1"),
					MaxKeys:             5,
					IsTruncated:         true,
				},
				{
					NextKeyMarker:       aws.String("testKey2"),
					NextVersionIdMarker: aws.String("testID2"),
					MaxKeys:             5,
					IsTruncated:         true,
				},
				{
					NextKeyMarker:       aws.String("testKey3"),
					NextVersionIdMarker: aws.String("testID3"),
					MaxKeys:             5,
					IsTruncated:         false,
				},
			},
		},
		"page limit 10 with duplicate marker": {
			testCase: testCase{
				bucket:                 aws.String("testBucket2"),
				limit:                  10,
				requestCnt:             3,
				stopOnDuplicationToken: true,
			},
			outputs: []*ListObjectVersionsOutput{
				{
					NextKeyMarker:       aws.String("testKey1"),
					NextVersionIdMarker: aws.String("testID1"),
					MaxKeys:             10,
					IsTruncated:         true,
				},
				{
					NextKeyMarker:       aws.String("testKey2"),
					NextVersionIdMarker: aws.String("testID2"),
					MaxKeys:             10,
					IsTruncated:         true,
				},
				{
					NextKeyMarker:       aws.String("testKey2"),
					NextVersionIdMarker: aws.String("testID2"),
					MaxKeys:             10,
					IsTruncated:         true,
				},
				{
					NextKeyMarker:       aws.String("testKey3"),
					NextVersionIdMarker: aws.String("testID3"),
					MaxKeys:             10,
					IsTruncated:         false,
				},
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			client := mockListObjectVersionsClient{
				t:       t,
				outputs: c.outputs,
				inputs:  []*ListObjectVersionsInput{},
			}
			paginator := NewListObjectVersionsPaginator(&client, &ListObjectVersionsInput{
				Bucket: c.bucket,
			}, func(options *ListObjectVersionsPaginatorOptions) {
				options.Limit = c.limit
				options.StopOnDuplicateToken = c.stopOnDuplicationToken
			})

			for paginator.HasMorePages() {
				_, err := paginator.NextPage(context.TODO())
				if err != nil {
					t.Errorf("error: %v", err)
				}
			}

			inputLen := len(client.inputs)
			testTotalRequests(c.requestCnt, inputLen, t)
			for i := 1; i < inputLen; i++ {
				if *client.inputs[i].KeyMarker != *c.outputs[i-1].NextKeyMarker {
					t.Errorf("Expect next input's KeyMarker to be eaqul to %s, got %s",
						*c.outputs[i-1].NextKeyMarker, *client.inputs[i].KeyMarker)
				}
				if *client.inputs[i].VersionIdMarker != *c.outputs[i-1].NextVersionIdMarker {
					t.Errorf("Expect next input's VersionIdMarker to be eaqul to %s, got %s",
						*c.outputs[i-1].NextVersionIdMarker, *client.inputs[i].VersionIdMarker)
				}
			}
		})
	}
}

func TestListMultipartUploadsPaginator(t *testing.T) {
	cases := map[string]listMPUTestCase{
		"page limit 5": {
			testCase: testCase{
				bucket:     aws.String("testBucket1"),
				limit:      5,
				requestCnt: 4,
			},
			outputs: []*ListMultipartUploadsOutput{
				{
					NextKeyMarker:      aws.String("testKey1"),
					NextUploadIdMarker: aws.String("testID1"),
					MaxUploads:         5,
					IsTruncated:        true,
				},
				{
					NextKeyMarker:      aws.String("testKey2"),
					NextUploadIdMarker: aws.String("testID2"),
					MaxUploads:         5,
					IsTruncated:        true,
				},
				{
					NextKeyMarker:      aws.String("testKey3"),
					NextUploadIdMarker: aws.String("testID3"),
					MaxUploads:         5,
					IsTruncated:        true,
				},
				{
					NextKeyMarker:      aws.String("testKey4"),
					NextUploadIdMarker: aws.String("testID4"),
					MaxUploads:         5,
					IsTruncated:        false,
				},
			},
		},
		"page limit 10 with duplicate marker": {
			testCase: testCase{
				bucket:                 aws.String("testBucket2"),
				limit:                  10,
				requestCnt:             3,
				stopOnDuplicationToken: true,
			},
			outputs: []*ListMultipartUploadsOutput{
				{
					NextKeyMarker:      aws.String("testKey1"),
					NextUploadIdMarker: aws.String("testID1"),
					MaxUploads:         10,
					IsTruncated:        true,
				},
				{
					NextKeyMarker:      aws.String("testKey2"),
					NextUploadIdMarker: aws.String("testID2"),
					MaxUploads:         10,
					IsTruncated:        true,
				},
				{
					NextKeyMarker:      aws.String("testKey2"),
					NextUploadIdMarker: aws.String("testID2"),
					MaxUploads:         10,
					IsTruncated:        true,
				},
				{
					NextKeyMarker:      aws.String("testKey4"),
					NextUploadIdMarker: aws.String("testID4"),
					MaxUploads:         10,
					IsTruncated:        false,
				},
				{
					NextKeyMarker:      aws.String("testKey5"),
					NextUploadIdMarker: aws.String("testID5"),
					MaxUploads:         10,
					IsTruncated:        false,
				},
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			client := mockListMultipartUploadsClient{
				outputs: c.outputs,
				inputs:  []*ListMultipartUploadsInput{},
				t:       t,
			}
			paginator := NewListMultipartUploadsPaginator(&client, &ListMultipartUploadsInput{
				Bucket: c.bucket,
			}, func(options *ListMultipartUploadsPaginatorOptions) {
				options.Limit = c.limit
				options.StopOnDuplicateToken = c.stopOnDuplicationToken
			})

			for paginator.HasMorePages() {
				_, err := paginator.NextPage(context.TODO())
				if err != nil {
					t.Errorf("error: %v", err)
				}
			}

			inputLen := len(client.inputs)
			testTotalRequests(c.requestCnt, inputLen, t)
			for i := 1; i < inputLen; i++ {
				if *client.inputs[i].KeyMarker != *c.outputs[i-1].NextKeyMarker {
					t.Errorf("Expect next input's KeyMarker to be eaqul to %s, got %s",
						*c.outputs[i-1].NextKeyMarker, *client.inputs[i].KeyMarker)
				}
				if *client.inputs[i].UploadIdMarker != *c.outputs[i-1].NextUploadIdMarker {
					t.Errorf("Expect next input's UploadIdMarker to be eaqul to %s, got %s",
						*c.outputs[i-1].NextUploadIdMarker, *client.inputs[i].UploadIdMarker)
				}
			}
		})
	}
}

func testTotalRequests(expect, actual int, t *testing.T) {
	if actual != expect {
		t.Errorf("Expect total request number to be %d, got %d", expect, actual)
	}
}

func testCurRequest(maxReqCnt, actualReqCnt int, expectLimit, actualLimit int32, t *testing.T) {
	if actualReqCnt > maxReqCnt {
		t.Errorf("Paginator calls client more than expected %d times", maxReqCnt)
	}
	if expectLimit != actualLimit {
		t.Errorf("Expect page limit to be %d, got %d", expectLimit, actualLimit)
	}
}

package stscreds_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/internal/sdk"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/sts/types"
)

type mockAssumeRoleWithWebIdentity func(ctx context.Context, params *sts.AssumeRoleWithWebIdentityInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleWithWebIdentityOutput, error)

func (m mockAssumeRoleWithWebIdentity) AssumeRoleWithWebIdentity(ctx context.Context, params *sts.AssumeRoleWithWebIdentityInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleWithWebIdentityOutput, error) {
	return m(ctx, params, optFns...)
}

type mockErrorCode string

func (m mockErrorCode) ErrorCode() string {
	return string(m)
}

func (m mockErrorCode) Error() string {
	return "error code: " + string(m)
}

func TestWebIdentityProviderRetrieve(t *testing.T) {
	restorTime := sdk.TestingUseReferenceTime(time.Time{})
	defer restorTime()

	cases := map[string]struct {
		mockClient        mockAssumeRoleWithWebIdentity
		roleARN           string
		tokenFilepath     string
		sessionName       string
		options           func(*stscreds.WebIdentityRoleOptions)
		expectedCredValue aws.Credentials
	}{
		"success": {
			roleARN:       "arn01234567890123456789",
			tokenFilepath: "testdata/token.jwt",
			options: func(o *stscreds.WebIdentityRoleOptions) {
				o.RoleSessionName = "foo"
			},
			mockClient: func(
				ctx context.Context, params *sts.AssumeRoleWithWebIdentityInput, optFns ...func(*sts.Options),
			) (
				*sts.AssumeRoleWithWebIdentityOutput, error,
			) {
				if e, a := "foo", *params.RoleSessionName; e != a {
					return nil, fmt.Errorf("expected %v, but received %v", e, a)
				}
				if params.DurationSeconds != nil {
					return nil, fmt.Errorf("expect no duration seconds, got %v",
						*params.DurationSeconds)
				}
				if params.Policy != nil {
					return nil, fmt.Errorf("expect no policy, got %v",
						*params.Policy)
				}
				return &sts.AssumeRoleWithWebIdentityOutput{
					Credentials: &types.Credentials{
						Expiration:      aws.Time(sdk.NowTime()),
						AccessKeyId:     aws.String("access-key-id"),
						SecretAccessKey: aws.String("secret-access-key"),
						SessionToken:    aws.String("session-token"),
					},
				}, nil
			},
			expectedCredValue: aws.Credentials{
				AccessKeyID:     "access-key-id",
				SecretAccessKey: "secret-access-key",
				SessionToken:    "session-token",
				Source:          stscreds.WebIdentityProviderName,
				CanExpire:       true,
				Expires:         sdk.NowTime(),
			},
		},
		"success with duration and policy": {
			roleARN:       "arn01234567890123456789",
			tokenFilepath: "testdata/token.jwt",
			options: func(o *stscreds.WebIdentityRoleOptions) {
				o.Duration = 42 * time.Second
				o.Policy = aws.String("super secret policy")
				o.RoleSessionName = "foo"
			},
			mockClient: func(
				ctx context.Context, params *sts.AssumeRoleWithWebIdentityInput, optFns ...func(*sts.Options),
			) (
				*sts.AssumeRoleWithWebIdentityOutput, error,
			) {
				if e, a := "foo", *params.RoleSessionName; e != a {
					return nil, fmt.Errorf("expected %v, but received %v", e, a)
				}
				if e, a := int32(42), aws.ToInt32(params.DurationSeconds); e != a {
					return nil, fmt.Errorf("expect %v duration seconds, got %v", e, a)
				}
				if e, a := "super secret policy", aws.ToString(params.Policy); e != a {
					return nil, fmt.Errorf("expect %v policy, got %v", e, a)
				}
				return &sts.AssumeRoleWithWebIdentityOutput{
					Credentials: &types.Credentials{
						Expiration:      aws.Time(sdk.NowTime()),
						AccessKeyId:     aws.String("access-key-id"),
						SecretAccessKey: aws.String("secret-access-key"),
						SessionToken:    aws.String("session-token"),
					},
				}, nil
			},
			expectedCredValue: aws.Credentials{
				AccessKeyID:     "access-key-id",
				SecretAccessKey: "secret-access-key",
				SessionToken:    "session-token",
				Source:          stscreds.WebIdentityProviderName,
				CanExpire:       true,
				Expires:         sdk.NowTime(),
			},
		},
		"configures token retry": {
			roleARN:       "arn01234567890123456789",
			tokenFilepath: "testdata/token.jwt",
			options: func(o *stscreds.WebIdentityRoleOptions) {
				o.RoleSessionName = "foo"
			},
			mockClient: func(
				ctx context.Context, params *sts.AssumeRoleWithWebIdentityInput, optFns ...func(*sts.Options),
			) (
				*sts.AssumeRoleWithWebIdentityOutput, error,
			) {
				o := sts.Options{}
				for _, fn := range optFns {
					fn(&o)
				}

				errorCode := (&types.InvalidIdentityTokenException{}).ErrorCode()
				if o.Retryer.IsErrorRetryable(mockErrorCode(errorCode)) != true {
					return nil, fmt.Errorf("expected %v to be retryable", errorCode)
				}

				return &sts.AssumeRoleWithWebIdentityOutput{
					Credentials: &types.Credentials{
						Expiration:      aws.Time(sdk.NowTime()),
						AccessKeyId:     aws.String("access-key-id"),
						SecretAccessKey: aws.String("secret-access-key"),
						SessionToken:    aws.String("session-token"),
					},
				}, nil
			},
			expectedCredValue: aws.Credentials{
				AccessKeyID:     "access-key-id",
				SecretAccessKey: "secret-access-key",
				SessionToken:    "session-token",
				Source:          stscreds.WebIdentityProviderName,
				CanExpire:       true,
				Expires:         sdk.NowTime(),
			},
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var optFns []func(*stscreds.WebIdentityRoleOptions)
			if c.options != nil {
				optFns = append(optFns, c.options)
			}
			p := stscreds.NewWebIdentityRoleProvider(
				c.mockClient,
				c.roleARN,
				stscreds.IdentityTokenFile(c.tokenFilepath),
				optFns...,
			)
			credValue, err := p.Retrieve(context.Background())
			if err != nil {
				t.Fatalf("expect no error, got %v", err)
			}

			if e, a := c.expectedCredValue, credValue; !reflect.DeepEqual(e, a) {
				t.Errorf("expected %v, but received %v", e, a)
			}
		})
	}
}

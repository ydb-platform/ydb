package stscreds_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/aws-sdk-go-v2/service/sts/types"
)

type mockAssumeRole struct {
	TestInput func(*sts.AssumeRoleInput)
}

func (s *mockAssumeRole) AssumeRole(ctx context.Context, params *sts.AssumeRoleInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleOutput, error) {
	if s.TestInput != nil {
		s.TestInput(params)
	}
	expiry := time.Now().Add(60 * time.Minute)

	return &sts.AssumeRoleOutput{
		Credentials: &types.Credentials{
			// Just reflect the role arn to the provider.
			AccessKeyId:     params.RoleArn,
			SecretAccessKey: aws.String("assumedSecretAccessKey"),
			SessionToken:    aws.String("assumedSessionToken"),
			Expiration:      &expiry,
		},
	}, nil
}

const roleARN = "00000000000000000000000000000000000"
const tokenCode = "00000000000000000000"

func TestAssumeRoleProvider(t *testing.T) {
	stub := &mockAssumeRole{}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN)

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Expect no error, %v", err)
	}

	if e, a := roleARN, creds.AccessKeyID; e != a {
		t.Errorf("Expect access key ID to be reflected role ARN")
	}
	if e, a := "assumedSecretAccessKey", creds.SecretAccessKey; e != a {
		t.Errorf("Expect secret access key to match")
	}
	if e, a := "assumedSessionToken", creds.SessionToken; e != a {
		t.Errorf("Expect session token to match")
	}
}

func TestAssumeRoleProvider_WithTokenProvider(t *testing.T) {
	stub := &mockAssumeRole{
		TestInput: func(in *sts.AssumeRoleInput) {
			if e, a := "0123456789", *in.SerialNumber; e != a {
				t.Errorf("expect %v, got %v", e, a)
			}
			if e, a := tokenCode, *in.TokenCode; e != a {
				t.Errorf("expect %v, got %v", e, a)
			}
		},
	}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN, func(options *stscreds.AssumeRoleOptions) {
		options.SerialNumber = aws.String("0123456789")
		options.TokenProvider = func() (string, error) {
			return tokenCode, nil
		}
	})

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Expect no error, %v", err)
	}

	if e, a := roleARN, creds.AccessKeyID; e != a {
		t.Errorf("Expect access key ID to be reflected role ARN")
	}
	if e, a := "assumedSecretAccessKey", creds.SecretAccessKey; e != a {
		t.Errorf("Expect secret access key to match")
	}
	if e, a := "assumedSessionToken", creds.SessionToken; e != a {
		t.Errorf("Expect session token to match")
	}
}

func TestAssumeRoleProvider_WithTokenProviderError(t *testing.T) {
	stub := &mockAssumeRole{
		TestInput: func(in *sts.AssumeRoleInput) {
			t.Fatalf("API request should not of been called")
		},
	}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN, func(options *stscreds.AssumeRoleOptions) {
		options.SerialNumber = aws.String("0123456789")
		options.TokenProvider = func() (string, error) {
			return "", fmt.Errorf("error occurred")
		}
	})

	creds, err := p.Retrieve(context.Background())
	if err == nil {
		t.Fatalf("expect error, got none")
	}

	if v := creds.AccessKeyID; len(v) != 0 {
		t.Errorf("expect zero, got %v", v)
	}
	if v := creds.SecretAccessKey; len(v) != 0 {
		t.Errorf("expect zero, got %v", v)
	}
	if v := creds.SessionToken; len(v) != 0 {
		t.Errorf("expect zero, got %v", v)
	}
}

func TestAssumeRoleProvider_MFAWithNoToken(t *testing.T) {
	stub := &mockAssumeRole{
		TestInput: func(in *sts.AssumeRoleInput) {
			t.Fatalf("API request should not of been called")
		},
	}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN, func(options *stscreds.AssumeRoleOptions) {
		options.SerialNumber = aws.String("0123456789")
	})

	creds, err := p.Retrieve(context.Background())
	if err == nil {
		t.Fatalf("expect error, got none")
	}

	if v := creds.AccessKeyID; len(v) != 0 {
		t.Errorf("expect zero, got %v", v)
	}
	if v := creds.SecretAccessKey; len(v) != 0 {
		t.Errorf("expect zero, got %v", v)
	}
	if v := creds.SessionToken; len(v) != 0 {
		t.Errorf("expect zero, got %v", v)
	}
}

func TestAssumeRoleProvider_WithSourceIdentity(t *testing.T) {
	const sourceIdentity = "Source-Identity"

	stub := &mockAssumeRole{
		TestInput: func(in *sts.AssumeRoleInput) {
			if e, a := sourceIdentity, *in.SourceIdentity; e != a {
				t.Fatalf("expect %v, got %v", e, a)
			}
		},
	}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN, func(options *stscreds.AssumeRoleOptions) {
		options.SourceIdentity = aws.String(sourceIdentity)
	})

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Expect no error, %v", err)
	}

	if e, a := roleARN, creds.AccessKeyID; e != a {
		t.Errorf("Expect access key ID to be reflected role ARN")
	}
	if e, a := "assumedSecretAccessKey", creds.SecretAccessKey; e != a {
		t.Errorf("Expect secret access key to match")
	}
	if e, a := "assumedSessionToken", creds.SessionToken; e != a {
		t.Errorf("Expect session token to match")
	}
}

func TestAssumeRoleProvider_WithTags(t *testing.T) {
	stub := &mockAssumeRole{
		TestInput: func(in *sts.AssumeRoleInput) {
			if e, a := 1, len(in.Tags); e != a {
				t.Fatalf("expect %v, got %v", e, a)
			}
			tag := in.Tags[0]
			if e, a := "KEY", *tag.Key; e != a {
				t.Errorf("expect %v, got %v", e, a)
			}
			if e, a := "value", *tag.Value; e != a {
				t.Errorf("expect %v, got %v", e, a)
			}
		},
	}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN, func(options *stscreds.AssumeRoleOptions) {
		options.Tags = []types.Tag{
			{
				Key:   aws.String("KEY"),
				Value: aws.String("value"),
			},
		}
	})

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Expect no error, %v", err)
	}

	if e, a := roleARN, creds.AccessKeyID; e != a {
		t.Errorf("Expect access key ID to be reflected role ARN")
	}
	if e, a := "assumedSecretAccessKey", creds.SecretAccessKey; e != a {
		t.Errorf("Expect secret access key to match")
	}
	if e, a := "assumedSessionToken", creds.SessionToken; e != a {
		t.Errorf("Expect session token to match")
	}
}

func TestAssumeRoleProvider_WithTransitiveTagKeys(t *testing.T) {
	stub := &mockAssumeRole{
		TestInput: func(in *sts.AssumeRoleInput) {
			if e, a := 1, len(in.TransitiveTagKeys); e != a {
				t.Fatalf("expect %v, got %v", e, a)
			}
			if e, a := "KEY", in.TransitiveTagKeys[0]; e != a {
				t.Errorf("expect %v, got %v", e, a)
			}
		},
	}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN, func(options *stscreds.AssumeRoleOptions) {
		options.Tags = []types.Tag{
			{
				Key:   aws.String("KEY"),
				Value: aws.String("value"),
			},
		}
		options.TransitiveTagKeys = []string{"KEY"}
	})

	creds, err := p.Retrieve(context.Background())
	if err != nil {
		t.Fatalf("Expect no error, %v", err)
	}

	if e, a := roleARN, creds.AccessKeyID; e != a {
		t.Errorf("Expect access key ID to be reflected role ARN")
	}
	if e, a := "assumedSecretAccessKey", creds.SecretAccessKey; e != a {
		t.Errorf("Expect secret access key to match")
	}
	if e, a := "assumedSessionToken", creds.SessionToken; e != a {
		t.Errorf("Expect session token to match")
	}
}

func BenchmarkAssumeRoleProvider(b *testing.B) {
	stub := &mockAssumeRole{}
	p := stscreds.NewAssumeRoleProvider(stub, roleARN)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Retrieve(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}

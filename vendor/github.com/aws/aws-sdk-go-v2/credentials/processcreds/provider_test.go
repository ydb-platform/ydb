package processcreds

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestProviderBadCommand(t *testing.T) {
	provider := NewProvider("/bad/process")
	_, err := provider.Retrieve(context.Background())
	var pe *ProviderError
	if ok := errors.As(err, &pe); !ok {
		t.Fatalf("expect error to be of type %T", pe)
	}
	if e, a := "error in credential_process", pe.Error(); !strings.Contains(a, e) {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestProviderMoreEmptyCommands(t *testing.T) {
	provider := NewProvider("")
	_, err := provider.Retrieve(context.Background())
	var pe *ProviderError
	if ok := errors.As(err, &pe); !ok {
		t.Fatalf("expect error to be of type %T", pe)
	}
	if e, a := "failed to prepare command", pe.Error(); !strings.Contains(a, e) {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestProviderExpectErrors(t *testing.T) {
	provider := NewProvider(
		fmt.Sprintf(
			"%s %s",
			getOSCat(),
			filepath.Join("testdata", "malformed.json"),
		))
	_, err := provider.Retrieve(context.Background())
	var pe *ProviderError
	if ok := errors.As(err, &pe); !ok {
		t.Fatalf("expect error to be of type %T", pe)
	}
	if e, a := "parse failed of process output", pe.Error(); !strings.Contains(a, e) {
		t.Errorf("expected %v, got %v", e, a)
	}

	provider = NewProvider(
		fmt.Sprintf("%s %s",
			getOSCat(),
			filepath.Join("testdata", "wrongversion.json"),
		))
	_, err = provider.Retrieve(context.Background())
	if ok := errors.As(err, &pe); !ok {
		t.Fatalf("expect error to be of type %T", pe)
	}
	if e, a := "wrong version in process output", pe.Error(); !strings.Contains(a, e) {
		t.Errorf("expected %v, got %v", e, a)
	}

	provider = NewProvider(
		fmt.Sprintf(
			"%s %s",
			getOSCat(),
			filepath.Join("testdata", "missingkey.json"),
		))
	_, err = provider.Retrieve(context.Background())
	if ok := errors.As(err, &pe); !ok {
		t.Fatalf("expect error to be of type %T", pe)
	}
	if e, a := "missing AccessKeyId", pe.Error(); !strings.Contains(a, e) {
		t.Errorf("expected %v, got %v", e, a)
	}

	provider = NewProvider(
		fmt.Sprintf(
			"%s %s",
			getOSCat(),
			filepath.Join("testdata", "missingsecret.json"),
		))
	_, err = provider.Retrieve(context.Background())
	if ok := errors.As(err, &pe); !ok {
		t.Fatalf("expect error to be of type %T", pe)
	}
	if e, a := "missing SecretAccessKey", pe.Error(); !strings.Contains(a, e) {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestProviderTimeout(t *testing.T) {
	command := "/bin/sleep 2"
	if runtime.GOOS == "windows" {
		// "timeout" command does not work due to pipe redirection
		command = "ping -n 2 127.0.0.1>nul"
	}

	provider := NewProvider(command, func(options *Options) {
		options.Timeout = time.Duration(1) * time.Second
	})
	_, err := provider.Retrieve(context.Background())
	var pe *ProviderError
	if ok := errors.As(err, &pe); !ok {
		t.Fatalf("expect error to be of type %T", pe)
	}
	if e, a := "credential process timed out", pe.Error(); !strings.Contains(a, e) {
		t.Errorf("expected %v, got %v", e, a)
	}
}

func TestProviderWithLongSessionToken(t *testing.T) {
	provider := NewProvider(
		fmt.Sprintf(
			"%s %s",
			getOSCat(),
			filepath.Join("testdata", "longsessiontoken.json"),
		))
	v, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}

	// Text string same length as session token returned by AWS for AssumeRoleWithWebIdentity
	e := "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
	if a := v.SessionToken; e != a {
		t.Errorf("expected %v, got %v", e, a)
	}
}

type credentialTest struct {
	Version         int
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string
	Expiration      string
}

func TestProviderStatic(t *testing.T) {
	// static
	provider := NewProvider(
		fmt.Sprintf(
			"%s %s",
			getOSCat(),
			filepath.Join("testdata", "static.json"),
		))
	v, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if v.CanExpire != false {
		t.Errorf("expected %v, got %v", "static credentials/not expired", "can expire")
	}

}

func TestProviderNotExpired(t *testing.T) {
	// non-static, not expired
	exp := &credentialTest{}
	exp.Version = 1
	exp.AccessKeyID = "accesskey"
	exp.SecretAccessKey = "secretkey"
	exp.Expiration = time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	b, err := json.Marshal(exp)
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}

	tmpFile, err := ioutil.TempFile(os.TempDir(), "tmp_expiring")
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if _, err = io.Copy(tmpFile, bytes.NewReader(b)); err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	defer func() {
		if err = tmpFile.Close(); err != nil {
			t.Errorf("expected %v, got %v", "no error", err)
		}
		if err = os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("expected %v, got %v", "no error", err)
		}
	}()
	provider := NewProvider(
		fmt.Sprintf("%s %s", getOSCat(), tmpFile.Name()))
	v, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if v.Expired() {
		t.Errorf("expected %v, got %v", "not expired", "expired")
	}
}

func TestProviderExpired(t *testing.T) {
	// non-static, expired
	exp := &credentialTest{}
	exp.Version = 1
	exp.AccessKeyID = "accesskey"
	exp.SecretAccessKey = "secretkey"
	exp.Expiration = time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	b, err := json.Marshal(exp)
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}

	tmpFile, err := ioutil.TempFile(os.TempDir(), "tmp_expired")
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if _, err = io.Copy(tmpFile, bytes.NewReader(b)); err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	defer func() {
		if err = tmpFile.Close(); err != nil {
			t.Errorf("expected %v, got %v", "no error", err)
		}
		if err = os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("expected %v, got %v", "no error", err)
		}
	}()
	provider := NewProvider(
		fmt.Sprintf("%s %s", getOSCat(), tmpFile.Name()))
	v, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if !v.Expired() {
		t.Errorf("expected %v, got %v", "expired", "not expired")
	}
}

func TestProviderForceExpire(t *testing.T) {
	// non-static, not expired

	// setup test credentials file
	exp := &credentialTest{}
	exp.Version = 1
	exp.AccessKeyID = "accesskey"
	exp.SecretAccessKey = "secretkey"
	exp.Expiration = time.Now().Add(1 * time.Hour).UTC().Format(time.RFC3339)
	b, err := json.Marshal(exp)
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	tmpFile, err := ioutil.TempFile(os.TempDir(), "tmp_force_expire")
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if _, err = io.Copy(tmpFile, bytes.NewReader(b)); err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	defer func() {
		if err = tmpFile.Close(); err != nil {
			t.Errorf("expected %v, got %v", "no error", err)
		}
		if err = os.Remove(tmpFile.Name()); err != nil {
			t.Errorf("expected %v, got %v", "no error", err)
		}
	}()

	// get credentials from file
	provider := NewProvider(
		fmt.Sprintf("%s %s", getOSCat(), tmpFile.Name()))
	v, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if v.Expired() {
		t.Errorf("expected %v, got %v", "not expired", "expired")
	}

	// Re-retrieve credentials
	v, err = provider.Retrieve(context.Background())
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if v.Expired() {
		t.Errorf("expected %v, got %v", "not expired", "expired")
	}
}

func TestProviderAltConstruct(t *testing.T) {
	cmdBuilder := DefaultNewCommandBuilder{Args: []string{
		fmt.Sprintf("%s %s", getOSCat(),
			filepath.Join("testdata", "static.json"),
		),
	}}

	provider := NewProviderCommand(cmdBuilder, func(options *Options) {
		options.Timeout = time.Duration(1) * time.Second
	})
	v, err := provider.Retrieve(context.Background())
	if err != nil {
		t.Errorf("expected %v, got %v", "no error", err)
	}
	if v.CanExpire != false {
		t.Errorf("expected %v, got %v", "static credentials/not expired", "expired")
	}
}

func BenchmarkProcessProvider(b *testing.B) {
	provider := NewProvider(
		fmt.Sprintf(
			"%s %s",
			getOSCat(),
			filepath.Join("testdata", "static.json"),
		))
	_, err := provider.Retrieve(context.Background())
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StartTimer()
		_, err := provider.Retrieve(context.Background())
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func getOSCat() string {
	if runtime.GOOS == "windows" {
		return "type"
	}
	return "cat"
}

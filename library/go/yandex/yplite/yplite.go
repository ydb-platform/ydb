package yplite

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"time"

	"a.yandex-team.ru/library/go/core/xerrors"
)

const (
	PodSocketPath    = "/run/iss/pod.socket"
	NodeAgentTimeout = 1 * time.Second
)

var (
	httpClient = http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.DialTimeout("unix", PodSocketPath, NodeAgentTimeout)
			},
		},
		Timeout: NodeAgentTimeout,
	}
)

func IsAPIAvailable() bool {
	if _, err := os.Stat(PodSocketPath); err == nil {
		return true
	}
	return false
}

func FetchPodSpec() (*PodSpec, error) {
	res, err := httpClient.Get("http://localhost/pod_spec")
	if err != nil {
		return nil, xerrors.Errorf("failed to request pod spec: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	spec := new(PodSpec)
	err = json.NewDecoder(res.Body).Decode(spec)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode pod spec: %w", err)
	}

	return spec, nil
}

func FetchPodAttributes() (*PodAttributes, error) {
	res, err := httpClient.Get("http://localhost/pod_attributes")
	if err != nil {
		return nil, xerrors.Errorf("failed to request pod attributes: %w", err)
	}
	defer func() { _ = res.Body.Close() }()

	attrs := new(PodAttributes)
	err = json.NewDecoder(res.Body).Decode(attrs)
	if err != nil {
		return nil, xerrors.Errorf("failed to decode pod attributes: %w", err)
	}

	return attrs, nil
}

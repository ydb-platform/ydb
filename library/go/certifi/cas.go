package certifi

import (
	"crypto/x509"
	"sync"

	"a.yandex-team.ru/library/go/certifi/internal/certs"
)

var (
	internalOnce sync.Once
	commonOnce   sync.Once
	internalCAs  []*x509.Certificate
	commonCAs    []*x509.Certificate
)

// InternalCAs returns list of Yandex Internal certificates
func InternalCAs() []*x509.Certificate {
	internalOnce.Do(initInternalCAs)
	return internalCAs
}

// CommonCAs returns list of common certificates
func CommonCAs() []*x509.Certificate {
	commonOnce.Do(initCommonCAs)
	return commonCAs
}

func initInternalCAs() {
	internalCAs = certsFromPEM(certs.InternalCAs())
}

func initCommonCAs() {
	commonCAs = certsFromPEM(certs.CommonCAs())
}

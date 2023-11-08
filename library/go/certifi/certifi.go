package certifi

import (
	"crypto/x509"
	"os"
)

var underYaMake = true

// NewCertPool returns a copy of the system or bundled cert pool.
//
// Default behavior can be modified with env variable, e.g. use system pool:
//
//	CERTIFI_USE_SYSTEM_CA=yes ./my-cool-program
func NewCertPool() (caCertPool *x509.CertPool, err error) {
	if forceSystem() {
		return NewCertPoolSystem()
	}

	return NewCertPoolBundled()
}

// NewCertPoolSystem returns a copy of the system cert pool + common CAs + internal CAs
//
// WARNING: system cert pool is not available on Windows
func NewCertPoolSystem() (caCertPool *x509.CertPool, err error) {
	caCertPool, err = x509.SystemCertPool()

	if err != nil || caCertPool == nil {
		caCertPool = x509.NewCertPool()
	}

	for _, cert := range CommonCAs() {
		caCertPool.AddCert(cert)
	}

	for _, cert := range InternalCAs() {
		caCertPool.AddCert(cert)
	}

	return caCertPool, nil
}

// NewCertPoolBundled returns a new cert pool with common CAs + internal CAs
func NewCertPoolBundled() (caCertPool *x509.CertPool, err error) {
	caCertPool = x509.NewCertPool()

	for _, cert := range CommonCAs() {
		caCertPool.AddCert(cert)
	}

	for _, cert := range InternalCAs() {
		caCertPool.AddCert(cert)
	}

	return caCertPool, nil
}

// NewCertPoolInternal returns a new cert pool with internal CAs
func NewCertPoolInternal() (caCertPool *x509.CertPool, err error) {
	caCertPool = x509.NewCertPool()

	for _, cert := range InternalCAs() {
		caCertPool.AddCert(cert)
	}

	return caCertPool, nil
}

func forceSystem() bool {
	if os.Getenv("CERTIFI_USE_SYSTEM_CA") == "yes" {
		return true
	}

	if !underYaMake && len(InternalCAs()) == 0 {
		return true
	}

	return false
}

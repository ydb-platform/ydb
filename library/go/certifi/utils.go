package certifi

import (
	"crypto/x509"
	"encoding/pem"
)

func certsFromPEM(pemCerts []byte) []*x509.Certificate {
	var result []*x509.Certificate
	for len(pemCerts) > 0 {
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}

		result = append(result, cert)
	}

	return result
}

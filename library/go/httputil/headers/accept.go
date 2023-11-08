package headers

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const (
	AcceptKey         = "Accept"
	AcceptEncodingKey = "Accept-Encoding"
)

type AcceptableEncodings []AcceptableEncoding

type AcceptableEncoding struct {
	Encoding ContentEncoding
	Weight   float32

	pos int
}

func (as AcceptableEncodings) IsAcceptable(encoding ContentEncoding) bool {
	for _, ae := range as {
		if ae.Encoding == encoding {
			return ae.Weight != 0
		}
	}
	return false
}

func (as AcceptableEncodings) String() string {
	if len(as) == 0 {
		return ""
	}

	var b strings.Builder
	for i, ae := range as {
		b.WriteString(ae.Encoding.String())

		if ae.Weight > 0.0 && ae.Weight < 1.0 {
			b.WriteString(";q=" + strconv.FormatFloat(float64(ae.Weight), 'f', 1, 32))
		}

		if i < len(as)-1 {
			b.WriteString(", ")
		}
	}
	return b.String()
}

type AcceptableTypes []AcceptableType

func (as AcceptableTypes) IsAcceptable(contentType ContentType) bool {
	for _, ae := range as {
		if ae.Type == contentType {
			return ae.Weight != 0
		}
	}
	return false
}

type AcceptableType struct {
	Type      ContentType
	Weight    float32
	Extension map[string]string

	pos int
}

func (as AcceptableTypes) String() string {
	if len(as) == 0 {
		return ""
	}

	var b strings.Builder
	for i, at := range as {
		b.WriteString(at.Type.String())

		if at.Weight > 0.0 && at.Weight < 1.0 {
			b.WriteString(";q=" + strconv.FormatFloat(float64(at.Weight), 'f', 1, 32))
		}

		for k, v := range at.Extension {
			b.WriteString(";" + k + "=" + v)
		}

		if i < len(as)-1 {
			b.WriteString(", ")
		}
	}
	return b.String()
}

// ParseAccept parses Accept HTTP header.
// It will sort acceptable types by weight, specificity and position.
// See: https://tools.ietf.org/html/rfc2616#section-14.1
func ParseAccept(headerValue string) (AcceptableTypes, error) {
	if headerValue == "" {
		return nil, nil
	}

	parsedValues, err := parseAcceptFamilyHeader(headerValue)
	if err != nil {
		return nil, err
	}
	ah := make(AcceptableTypes, 0, len(parsedValues))
	for _, parsedValue := range parsedValues {
		ah = append(ah, AcceptableType{
			Type:      ContentType(parsedValue.Value),
			Weight:    parsedValue.Weight,
			Extension: parsedValue.Extension,
			pos:       parsedValue.pos,
		})
	}

	sort.Slice(ah, func(i, j int) bool {
		// sort by weight only
		if ah[i].Weight != ah[j].Weight {
			return ah[i].Weight > ah[j].Weight
		}

		// sort by most specific if types are equal
		if ah[i].Type == ah[j].Type {
			return len(ah[i].Extension) > len(ah[j].Extension)
		}

		// move counterpart up if one of types is ANY
		if ah[i].Type == ContentTypeAny {
			return false
		}
		if ah[j].Type == ContentTypeAny {
			return true
		}

		// i type has j type as prefix
		if strings.HasSuffix(string(ah[j].Type), "/*") &&
			strings.HasPrefix(string(ah[i].Type), string(ah[j].Type)[:len(ah[j].Type)-1]) {
			return true
		}

		// j type has i type as prefix
		if strings.HasSuffix(string(ah[i].Type), "/*") &&
			strings.HasPrefix(string(ah[j].Type), string(ah[i].Type)[:len(ah[i].Type)-1]) {
			return false
		}

		// sort by position if nothing else left
		return ah[i].pos < ah[j].pos
	})

	return ah, nil
}

// ParseAcceptEncoding parses Accept-Encoding HTTP header.
// It will sort acceptable encodings by weight and position.
// See: https://tools.ietf.org/html/rfc2616#section-14.3
func ParseAcceptEncoding(headerValue string) (AcceptableEncodings, error) {
	if headerValue == "" {
		return nil, nil
	}

	// e.g. gzip;q=1.0, compress, identity
	parsedValues, err := parseAcceptFamilyHeader(headerValue)
	if err != nil {
		return nil, err
	}
	acceptableEncodings := make(AcceptableEncodings, 0, len(parsedValues))
	for _, parsedValue := range parsedValues {
		acceptableEncodings = append(acceptableEncodings, AcceptableEncoding{
			Encoding: ContentEncoding(parsedValue.Value),
			Weight:   parsedValue.Weight,
			pos:      parsedValue.pos,
		})
	}
	sort.Slice(acceptableEncodings, func(i, j int) bool {
		// sort by weight only
		if acceptableEncodings[i].Weight != acceptableEncodings[j].Weight {
			return acceptableEncodings[i].Weight > acceptableEncodings[j].Weight
		}

		// move counterpart up if one of encodings is ANY
		if acceptableEncodings[i].Encoding == EncodingAny {
			return false
		}
		if acceptableEncodings[j].Encoding == EncodingAny {
			return true
		}

		// sort by position if nothing else left
		return acceptableEncodings[i].pos < acceptableEncodings[j].pos
	})

	return acceptableEncodings, nil
}

type acceptHeaderValue struct {
	Value     string
	Weight    float32
	Extension map[string]string

	pos int
}

// parseAcceptFamilyHeader parses family of Accept* HTTP headers
// See: https://tools.ietf.org/html/rfc2616#section-14.1
func parseAcceptFamilyHeader(header string) ([]acceptHeaderValue, error) {
	headerValues := strings.Split(header, ",")

	parsedValues := make([]acceptHeaderValue, 0, len(headerValues))
	for i, headerValue := range headerValues {
		valueParams := strings.Split(headerValue, ";")

		parsedValue := acceptHeaderValue{
			Value:  strings.TrimSpace(valueParams[0]),
			Weight: 1.0,
			pos:    i,
		}

		// parse quality factor and/or accept extension
		if len(valueParams) > 1 {
			for _, rawParam := range valueParams[1:] {
				rawParam = strings.TrimSpace(rawParam)
				params := strings.SplitN(rawParam, "=", 2)
				key := strings.TrimSpace(params[0])

				// quality factor
				if key == "q" {
					if len(params) != 2 {
						return nil, fmt.Errorf("invalid quality factor format: %q", rawParam)
					}

					w, err := strconv.ParseFloat(params[1], 32)
					if err != nil {
						return nil, err
					}
					parsedValue.Weight = float32(w)

					continue
				}

				// extension
				if parsedValue.Extension == nil {
					parsedValue.Extension = make(map[string]string)
				}

				var value string
				if len(params) == 2 {
					value = strings.TrimSpace(params[1])
				}
				parsedValue.Extension[key] = value
			}
		}

		parsedValues = append(parsedValues, parsedValue)
	}
	return parsedValues, nil
}

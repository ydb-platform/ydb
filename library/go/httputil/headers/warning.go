package headers

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"a.yandex-team.ru/library/go/core/xerrors"
)

const (
	WarningKey = "Warning"

	WarningResponseIsStale                = 110 // RFC 7234, 5.5.1
	WarningRevalidationFailed             = 111 // RFC 7234, 5.5.2
	WarningDisconnectedOperation          = 112 // RFC 7234, 5.5.3
	WarningHeuristicExpiration            = 113 // RFC 7234, 5.5.4
	WarningMiscellaneousWarning           = 199 // RFC 7234, 5.5.5
	WarningTransformationApplied          = 214 // RFC 7234, 5.5.6
	WarningMiscellaneousPersistentWarning = 299 // RFC 7234, 5.5.7
)

var warningStatusText = map[int]string{
	WarningResponseIsStale:                "Response is Stale",
	WarningRevalidationFailed:             "Revalidation Failed",
	WarningDisconnectedOperation:          "Disconnected Operation",
	WarningHeuristicExpiration:            "Heuristic Expiration",
	WarningMiscellaneousWarning:           "Miscellaneous Warning",
	WarningTransformationApplied:          "Transformation Applied",
	WarningMiscellaneousPersistentWarning: "Miscellaneous Persistent Warning",
}

// WarningText returns a text for the warning header code. It returns the empty
// string if the code is unknown.
func WarningText(warn int) string {
	return warningStatusText[warn]
}

// AddWarning adds Warning to http.Header with proper formatting
// see: https://tools.ietf.org/html/rfc7234#section-5.5
func AddWarning(h http.Header, warn int, agent, reason string, date time.Time) {
	values := make([]string, 0, 4)
	values = append(values, strconv.Itoa(warn))

	if agent != "" {
		values = append(values, agent)
	} else {
		values = append(values, "-")
	}

	if reason != "" {
		values = append(values, strconv.Quote(reason))
	}

	if !date.IsZero() {
		values = append(values, strconv.Quote(date.Format(time.RFC1123)))
	}

	h.Add(WarningKey, strings.Join(values, " "))
}

type WarningHeader struct {
	Code   int
	Agent  string
	Reason string
	Date   time.Time
}

// ParseWarnings reads and parses Warning headers from http.Header
func ParseWarnings(h http.Header) ([]WarningHeader, error) {
	warnings, ok := h[WarningKey]
	if !ok {
		return nil, nil
	}

	res := make([]WarningHeader, 0, len(warnings))
	for _, warn := range warnings {
		wh, err := parseWarning(warn)
		if err != nil {
			return nil, xerrors.Errorf("cannot parse '%s' header: %w", warn, err)
		}
		res = append(res, wh)
	}

	return res, nil
}

func parseWarning(warn string) (WarningHeader, error) {
	var res WarningHeader

	// parse code
	{
		codeSP := strings.Index(warn, " ")

		// fast path - code only warning
		if codeSP == -1 {
			code, err := strconv.Atoi(warn)
			res.Code = code
			return res, err
		}

		code, err := strconv.Atoi(warn[:codeSP])
		if err != nil {
			return WarningHeader{}, err
		}
		res.Code = code

		warn = strings.TrimSpace(warn[codeSP+1:])
	}

	// parse agent
	{
		agentSP := strings.Index(warn, " ")

		// fast path - no data after agent
		if agentSP == -1 {
			res.Agent = warn
			return res, nil
		}

		res.Agent = warn[:agentSP]
		warn = strings.TrimSpace(warn[agentSP+1:])
	}

	// parse reason
	{
		if len(warn) == 0 {
			return res, nil
		}

		// reason must by quoted, so we search for second quote
		reasonSP := strings.Index(warn[1:], `"`)

		// fast path - bad reason
		if reasonSP == -1 {
			return WarningHeader{}, errors.New("bad reason formatting")
		}

		res.Reason = warn[1 : reasonSP+1]
		warn = strings.TrimSpace(warn[reasonSP+2:])
	}

	// parse date
	{
		if len(warn) == 0 {
			return res, nil
		}

		// optional date must by quoted, so we search for second quote
		dateSP := strings.Index(warn[1:], `"`)

		// fast path - bad date
		if dateSP == -1 {
			return WarningHeader{}, errors.New("bad date formatting")
		}

		dt, err := time.Parse(time.RFC1123, warn[1:dateSP+1])
		if err != nil {
			return WarningHeader{}, err
		}
		res.Date = dt
	}

	return res, nil
}

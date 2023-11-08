package headers

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWarningText(t *testing.T) {
	testCases := []struct {
		code   int
		expect string
	}{
		{WarningResponseIsStale, "Response is Stale"},
		{WarningRevalidationFailed, "Revalidation Failed"},
		{WarningDisconnectedOperation, "Disconnected Operation"},
		{WarningHeuristicExpiration, "Heuristic Expiration"},
		{WarningMiscellaneousWarning, "Miscellaneous Warning"},
		{WarningTransformationApplied, "Transformation Applied"},
		{WarningMiscellaneousPersistentWarning, "Miscellaneous Persistent Warning"},
		{42, ""},
		{1489, ""},
	}

	for _, tc := range testCases {
		t.Run(strconv.Itoa(tc.code), func(t *testing.T) {
			assert.Equal(t, tc.expect, WarningText(tc.code))
		})
	}
}

func TestAddWarning(t *testing.T) {
	type args struct {
		warn   int
		agent  string
		reason string
		date   time.Time
	}

	testCases := []struct {
		name   string
		args   args
		expect http.Header
	}{
		{
			name: "code_only",
			args: args{warn: WarningResponseIsStale, agent: "", reason: "", date: time.Time{}},
			expect: http.Header{
				WarningKey: []string{
					"110 -",
				},
			},
		},
		{
			name: "code_agent",
			args: args{warn: WarningResponseIsStale, agent: "ololo/trololo", reason: "", date: time.Time{}},
			expect: http.Header{
				WarningKey: []string{
					"110 ololo/trololo",
				},
			},
		},
		{
			name: "code_agent_reason",
			args: args{warn: WarningResponseIsStale, agent: "ololo/trololo", reason: "shimba-boomba", date: time.Time{}},
			expect: http.Header{
				WarningKey: []string{
					`110 ololo/trololo "shimba-boomba"`,
				},
			},
		},
		{
			name: "code_agent_reason_date",
			args: args{
				warn:   WarningResponseIsStale,
				agent:  "ololo/trololo",
				reason: "shimba-boomba",
				date:   time.Date(2019, time.January, 14, 10, 50, 43, 0, time.UTC),
			},
			expect: http.Header{
				WarningKey: []string{
					`110 ololo/trololo "shimba-boomba" "Mon, 14 Jan 2019 10:50:43 UTC"`,
				},
			},
		},
		{
			name: "code_reason_date",
			args: args{
				warn:   WarningResponseIsStale,
				agent:  "",
				reason: "shimba-boomba",
				date:   time.Date(2019, time.January, 14, 10, 50, 43, 0, time.UTC),
			},
			expect: http.Header{
				WarningKey: []string{
					`110 - "shimba-boomba" "Mon, 14 Jan 2019 10:50:43 UTC"`,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.Header{}
			AddWarning(h, tc.args.warn, tc.args.agent, tc.args.reason, tc.args.date)
			assert.Equal(t, tc.expect, h)
		})
	}
}

func TestParseWarnings(t *testing.T) {
	testCases := []struct {
		name      string
		h         http.Header
		expect    []WarningHeader
		expectErr bool
	}{
		{
			name:      "no_warnings",
			h:         http.Header{},
			expect:    nil,
			expectErr: false,
		},
		{
			name: "single_code_only",
			h: http.Header{
				WarningKey: []string{
					"110",
				},
			},
			expect: []WarningHeader{
				{
					Code:   110,
					Agent:  "",
					Reason: "",
					Date:   time.Time{},
				},
			},
		},
		{
			name: "single_code_and_empty_agent",
			h: http.Header{
				WarningKey: []string{
					"110 -",
				},
			},
			expect: []WarningHeader{
				{
					Code:   110,
					Agent:  "-",
					Reason: "",
					Date:   time.Time{},
				},
			},
		},
		{
			name: "single_code_and_agent",
			h: http.Header{
				WarningKey: []string{
					"110 shimba/boomba",
				},
			},
			expect: []WarningHeader{
				{
					Code:   110,
					Agent:  "shimba/boomba",
					Reason: "",
					Date:   time.Time{},
				},
			},
		},
		{
			name: "single_code_agent_and_reason",
			h: http.Header{
				WarningKey: []string{
					`110 shimba/boomba "looken tooken"`,
				},
			},
			expect: []WarningHeader{
				{
					Code:   110,
					Agent:  "shimba/boomba",
					Reason: "looken tooken",
					Date:   time.Time{},
				},
			},
		},
		{
			name: "single_full",
			h: http.Header{
				WarningKey: []string{
					`110 shimba/boomba "looken tooken" "Mon, 14 Jan 2019 10:50:43 UTC"`,
				},
			},
			expect: []WarningHeader{
				{
					Code:   110,
					Agent:  "shimba/boomba",
					Reason: "looken tooken",
					Date:   time.Date(2019, time.January, 14, 10, 50, 43, 0, time.UTC),
				},
			},
		},
		{
			name: "multiple_full",
			h: http.Header{
				WarningKey: []string{
					`110 shimba/boomba "looken tooken" "Mon, 14 Jan 2019 10:50:43 UTC"`,
					`112 chiken "cooken" "Mon, 15 Jan 2019 10:51:43 UTC"`,
				},
			},
			expect: []WarningHeader{
				{
					Code:   110,
					Agent:  "shimba/boomba",
					Reason: "looken tooken",
					Date:   time.Date(2019, time.January, 14, 10, 50, 43, 0, time.UTC),
				},
				{
					Code:   112,
					Agent:  "chiken",
					Reason: "cooken",
					Date:   time.Date(2019, time.January, 15, 10, 51, 43, 0, time.UTC),
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseWarnings(tc.h)

			if tc.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.expect, got)
		})
	}
}

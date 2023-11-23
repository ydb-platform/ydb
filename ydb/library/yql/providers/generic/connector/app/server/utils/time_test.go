package utils

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimeToYDBDate(t *testing.T) {
	type testCase struct {
		input  time.Time
		output uint16
		err    error
	}

	tcs := []testCase{
		{
			input:  time.Date(1970, 01, 01, 00, 00, 00, 00, time.UTC),
			output: 0,
			err:    nil,
		},
		{
			input:  time.Date(1970, 01, 02, 00, 00, 00, 00, time.UTC),
			output: 1,
			err:    nil,
		},
		{
			input:  time.Date(1969, 12, 31, 23, 59, 00, 00, time.UTC),
			output: 0,
			err:    ErrValueOutOfTypeBounds,
		},
		{
			input:  time.Date(9999, 01, 01, 00, 00, 00, 00, time.UTC),
			output: 0,
			err:    ErrValueOutOfTypeBounds,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.input.String(), func(t *testing.T) {
			output, err := TimeToYDBDate(&tc.input)
			require.Equal(t, tc.output, output)

			if tc.err != nil {
				require.True(t, errors.Is(tc.err, ErrValueOutOfTypeBounds))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTimeToYDBDatetime(t *testing.T) {
	type testCase struct {
		input  time.Time
		output uint32
		err    error
	}

	tcs := []testCase{
		{
			input:  time.Date(1970, 01, 01, 00, 00, 00, 00, time.UTC),
			output: 0,
			err:    nil,
		},
		{
			input:  time.Date(1970, 01, 02, 00, 00, 00, 00, time.UTC),
			output: 86400,
			err:    nil,
		},
		{
			input:  time.Date(1969, 12, 31, 23, 59, 00, 00, time.UTC),
			output: 0,
			err:    ErrValueOutOfTypeBounds,
		},
		{
			input:  time.Date(9999, 01, 01, 00, 00, 00, 00, time.UTC),
			output: 0,
			err:    ErrValueOutOfTypeBounds,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.input.String(), func(t *testing.T) {
			output, err := TimeToYDBDatetime(&tc.input)
			require.Equal(t, tc.output, output)

			if tc.err != nil {
				require.True(t, errors.Is(tc.err, ErrValueOutOfTypeBounds))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTimeToYDBTimestamp(t *testing.T) {
	type testCase struct {
		input  time.Time
		output uint64
		err    error
	}

	tcs := []testCase{
		{
			input:  time.Date(1970, 01, 01, 00, 00, 00, 00, time.UTC),
			output: 0,
			err:    nil,
		},
		{
			input:  time.Date(1970, 01, 02, 00, 00, 00, 00, time.UTC),
			output: 86400000000,
			err:    nil,
		},
		{
			input:  time.Date(1969, 12, 31, 23, 59, 00, 00, time.UTC),
			output: 0,
			err:    ErrValueOutOfTypeBounds,
		},
		{
			input:  time.Date(29427, 01, 01, 00, 00, 00, 00, time.UTC),
			output: 0,
			err:    ErrValueOutOfTypeBounds,
		},
	}

	for _, tc := range tcs {
		tc := tc

		t.Run(tc.input.String(), func(t *testing.T) {
			output, err := TimeToYDBTimestamp(&tc.input)
			require.Equal(t, tc.output, output)

			if tc.err != nil {
				require.True(t, errors.Is(tc.err, ErrValueOutOfTypeBounds))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

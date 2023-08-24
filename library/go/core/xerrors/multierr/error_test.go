package multierr

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/xerrors"
)

func TestCombine(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		GivenErrors   []error
		ExpectedError error
	}{
		{
			GivenErrors:   nil,
			ExpectedError: nil,
		},
		{
			GivenErrors:   []error{},
			ExpectedError: nil,
		},
		{
			GivenErrors: []error{
				errors.New("foo"),
				nil,
				errors.New("bar"),
				nil,
			},
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			GivenErrors: []error{
				errors.New("foo"),
				newMultiError(errors.New("bar")),
			},
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			GivenErrors:   []error{errors.New("something wrong")},
			ExpectedError: errors.New("something wrong"),
		},
		{
			GivenErrors: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			GivenErrors: []error{
				errors.New("something"),
				errors.New("O\n O\n P\n S\n"),
				errors.New("wrong"),
			},
			ExpectedError: newMultiError(
				errors.New("something"),
				errors.New("O\n O\n P\n S\n"),
				errors.New("wrong"),
			),
		},
		{
			GivenErrors: []error{
				errors.New("foo"),
				newMultiError(
					errors.New("bar"),
					errors.New("baz"),
				),
				errors.New("qyz"),
			},
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
				errors.New("baz"),
				errors.New("qyz"),
			),
		},
		{
			GivenErrors: []error{
				errors.New("foo"),
				nil,
				newMultiError(
					errors.New("bar"),
				),
				nil,
			},
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			GivenErrors: []error{
				errors.New("foo"),
				newMultiError(
					errors.New("bar"),
				),
			},
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
	}

	for i, c := range testCases {
		c := c

		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			require.Equal(t, c.ExpectedError, Combine(c.GivenErrors...))
		})
	}
}

func TestFormatWithoutTraces(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		GivenError              error
		ExpectedSingleLineError string
		ExpectedMultiLineError  string
	}{
		{
			GivenError:              Combine(errors.New("foo")),
			ExpectedSingleLineError: "foo",
			ExpectedMultiLineError:  "foo",
		},
		{
			GivenError: Combine(
				errors.New("foo"),
				errors.New("bar"),
			),
			ExpectedSingleLineError: "foo; bar",
			ExpectedMultiLineError: "" +
				"foo\n" +
				"bar",
		},
		{
			GivenError: Combine(
				errors.New("foo"),
				errors.New("bar"),
				errors.New("baz"),
				errors.New("qyz"),
			),
			ExpectedSingleLineError: "foo; bar; baz; qyz",
			ExpectedMultiLineError: "" +
				"foo\n" +
				"bar\n" +
				"baz\n" +
				"qyz",
		},
		{
			GivenError: Combine(
				errors.New("something"),
				errors.New("O\n O\n P\n S\n"),
				errors.New("wrong"),
			),
			ExpectedSingleLineError: "something; O\n O\n P\n S\n; wrong",
			ExpectedMultiLineError: "" +
				"something\n" +
				"O\n" +
				"   O\n" +
				"   P\n" +
				"   S\n\n" +
				"wrong",
		},
	}

	for i, c := range testCases {
		c := c

		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			t.Run("sprint/single_line", func(t *testing.T) {
				assert.Equal(t, c.ExpectedSingleLineError, fmt.Sprintf("%v", c.GivenError))
			})

			t.Run("error", func(t *testing.T) {
				assert.Equal(t, c.ExpectedSingleLineError, c.GivenError.Error())
			})

			t.Run("sprintf/multi_line", func(t *testing.T) {
				assert.Equal(t, c.ExpectedMultiLineError, fmt.Sprintf("%+v", c.GivenError))
			})
		})
	}
}

func TestCombineDoesNotModifySlice(t *testing.T) {
	t.Parallel()

	errs := []error{
		errors.New("foo"),
		nil,
		errors.New("bar"),
	}

	assert.Error(t, Combine(errs...))
	assert.Len(t, errs, 3)
	assert.NoError(t, errs[1])
}

func TestAppend(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		GivenLeftError  error
		GivenRightError error
		ExpectedError   error
	}{
		{
			GivenLeftError:  nil,
			GivenRightError: nil,
			ExpectedError:   nil,
		},
		{
			GivenLeftError:  nil,
			GivenRightError: errors.New("something wrong"),
			ExpectedError:   errors.New("something wrong"),
		},
		{
			GivenLeftError:  errors.New("something wrong"),
			GivenRightError: nil,
			ExpectedError:   errors.New("something wrong"),
		},
		{
			GivenLeftError:  errors.New("foo"),
			GivenRightError: errors.New("bar"),
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			GivenLeftError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
			GivenRightError: errors.New("baz"),
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
				errors.New("baz"),
			),
		},
		{
			GivenLeftError: errors.New("baz"),
			GivenRightError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
			ExpectedError: newMultiError(
				errors.New("baz"),
				errors.New("foo"),
				errors.New("bar"),
			),
		},
		{
			GivenLeftError: newMultiError(
				errors.New("foo"),
			),
			GivenRightError: newMultiError(
				errors.New("bar"),
			),
			ExpectedError: newMultiError(
				errors.New("foo"),
				errors.New("bar"),
			),
		},
	}

	for i, c := range testCases {
		c := c
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			err := Append(c.GivenLeftError, c.GivenRightError)

			require.Equal(t, c.ExpectedError, err)
		})
	}
}

func TestAppendDoesNotModify(t *testing.T) {
	t.Parallel()

	var (
		initial = newMultiErrorWithCapacity()
		foo     = Append(initial, errors.New("foo"))
		bar     = Append(initial, errors.New("bar"))
	)

	t.Run("initial_not_modified", func(t *testing.T) {
		t.Parallel()

		assert.EqualError(t, initial, newMultiErrorWithCapacity().Error())
	})

	t.Run("errors_appended", func(t *testing.T) {
		t.Parallel()

		assert.EqualError(t, bar, Append(newMultiErrorWithCapacity(), errors.New("bar")).Error())
		assert.EqualError(t, foo, Append(newMultiErrorWithCapacity(), errors.New("foo")).Error())
	})

	t.Run("errors_len_equal", func(t *testing.T) {
		t.Parallel()

		assert.Len(t, Errors(foo), len(Errors(bar)))
		assert.Len(t, Errors(foo), len(Errors(initial))+1)
	})
}

func TestErrors(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		Given    error
		Expected []error
		Cast     bool
	}{
		{
			Given:    nil,
			Expected: nil,
		},
		{
			Given:    errors.New("go"),
			Expected: []error{errors.New("go")},
		},
		{
			Given:    groupNotMultiError{},
			Expected: groupNotMultiError{}.Errors(),
		},
		{
			Given: Combine(
				errors.New("foo"),
				errors.New("bar"),
			),
			Expected: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
			Cast: true,
		},
		{
			Given: Append(
				errors.New("foo"),
				errors.New("bar"),
			),
			Expected: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
			Cast: true,
		},
		{
			Given: Append(
				errors.New("foo"),
				Combine(
					errors.New("bar"),
				),
			),
			Expected: []error{
				errors.New("foo"),
				errors.New("bar"),
			},
		},
		{
			Given: Combine(
				errors.New("foo"),
				Append(
					errors.New("bar"),
					errors.New("baz"),
				),
				errors.New("qux"),
			),
			Expected: []error{
				errors.New("foo"),
				errors.New("bar"),
				errors.New("baz"),
				errors.New("qux"),
			},
		},
	}

	for i, c := range testCases {
		c := c

		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			t.Run("errors", func(t *testing.T) {
				require.Equal(t, c.Expected, Errors(c.Given))
			})

			if !c.Cast {
				return
			}

			t.Run("multiError/errors", func(t *testing.T) {
				require.Equal(t, c.Expected, c.Given.(*multiError).Errors())
			})

			t.Run("errorGroup/errors", func(t *testing.T) {
				require.Equal(t, c.Expected, c.Given.(errorGroup).Errors())
			})
		})
	}
}

func TestAppendRace(t *testing.T) {
	t.Parallel()

	initial := newMultiErrorWithCapacity()

	require.NotPanics(t, func() {
		var wg sync.WaitGroup

		for i := 0; i < 10; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				err := initial
				for j := 0; j < 10; j++ {
					err = Append(err, errors.New("foo"))
				}
			}()
		}

		wg.Wait()
	})
}

func TestErrorsSliceIsImmutable(t *testing.T) {
	t.Parallel()

	var (
		foo = errors.New("foo")
		bar = errors.New("bar")
	)

	err := Append(foo, bar)
	actualErrors := Errors(err)
	require.Equal(t, []error{foo, bar}, actualErrors)

	actualErrors[0] = nil
	actualErrors[1] = errors.New("bax")

	require.Equal(t, []error{foo, bar}, Errors(err))
}

func TestNilMultiError(t *testing.T) {
	t.Parallel()

	var err *multiError

	require.Empty(t, err.Error())
	require.Empty(t, err.Errors())
}

var (
	errFoo    = errors.New("foo")
	errBar    = errors.New("bar")
	errAbsent = errors.New("absent")
)

func TestIsMultiError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		GivenError  error
		GivenTarget error
		ExpectedIs  bool
	}{
		{
			GivenError:  nil,
			GivenTarget: nil,
			ExpectedIs:  true,
		},
		{
			GivenError:  nil,
			GivenTarget: errFoo,
			ExpectedIs:  false,
		},
		{
			GivenError:  Combine(errFoo),
			GivenTarget: nil,
			ExpectedIs:  false,
		},
		{
			GivenError:  Combine(errFoo),
			GivenTarget: errFoo,
			ExpectedIs:  true,
		},
		{
			GivenError:  Append(errFoo, errBar),
			GivenTarget: errFoo,
			ExpectedIs:  true,
		},
		{
			GivenError:  Append(errFoo, errBar),
			GivenTarget: errBar,
			ExpectedIs:  true,
		},
		{
			GivenError:  Append(errFoo, errBar),
			GivenTarget: errAbsent,
			ExpectedIs:  false,
		},
	}

	for i, c := range testCases {
		c := c

		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			if err, ok := c.GivenError.(*multiError); ok {
				t.Run("is", func(t *testing.T) {
					assert.Equal(t, c.ExpectedIs, err.Is(c.GivenTarget))
				})
			}

			t.Run("errors", func(t *testing.T) {
				assert.Equal(t, c.ExpectedIs, errors.Is(c.GivenError, c.GivenTarget))
			})

			t.Run("xerrors", func(t *testing.T) {
				assert.Equal(t, c.ExpectedIs, xerrors.Is(c.GivenError, c.GivenTarget))
			})
		})
	}
}

func TestAsMultiError(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		GivenError error
		ExpectedAs bool
	}{
		{
			GivenError: nil,
			ExpectedAs: false,
		},
		{
			GivenError: Combine(targetError{}),
			ExpectedAs: true,
		},
		{
			GivenError: Combine(mockedError{}),
			ExpectedAs: false,
		},
		{
			GivenError: Append(mockedError{}, targetError{}),
			ExpectedAs: true,
		},
		{
			GivenError: Append(mockedError{}, groupNotMultiError{}),
			ExpectedAs: false,
		},
	}

	for i, c := range testCases {
		c := c

		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()

			var target targetError

			if err, ok := c.GivenError.(*multiError); ok {
				t.Run("as", func(t *testing.T) {
					assert.Equal(t, c.ExpectedAs, err.As(&target))
				})
			}

			t.Run("errors", func(t *testing.T) {
				assert.Equal(t, c.ExpectedAs, errors.As(c.GivenError, &target))
			})

			t.Run("xerrors", func(t *testing.T) {
				assert.Equal(t, c.ExpectedAs, xerrors.As(c.GivenError, &target))
			})
		})
	}
}

func newMultiError(errors ...error) error {
	return &multiError{errors: errors}
}

func newMultiErrorWithCapacity() error {
	return appendN(nil, errors.New("append"), 50)
}

func appendN(initial, err error, repeat int) error {
	errs := initial

	for i := 0; i < repeat; i++ {
		errs = Append(errs, err)
	}

	return errs
}

type groupNotMultiError struct{}

func (e groupNotMultiError) Error() string {
	return "something wrong"
}

func (e groupNotMultiError) Errors() []error {
	return []error{errors.New("something wrong")}
}

type mockedError struct{}

func (e mockedError) Error() string {
	return "mocked"
}

type targetError struct{}

func (e targetError) Error() string {
	return "target"
}

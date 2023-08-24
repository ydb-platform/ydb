package xerrors

import (
	"errors"
	"fmt"
	"testing"

	pkgerrors "github.com/pkg/errors"
	"github.com/ydb-platform/ydb/library/go/core/xerrors/benchxerrors"
	"github.com/ydb-platform/ydb/library/go/test/testhelpers"
	"golang.org/x/xerrors"
)

const (
	benchNewMsg    = "foo"
	benchErrorfMsg = "bar: %w"
)

func BenchmarkNewStd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = errors.New(benchNewMsg)
	}
}

func BenchmarkNewPkg(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = pkgerrors.New(benchNewMsg)
	}
}

func BenchmarkNewXerrors(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = xerrors.New(benchNewMsg)
	}
}

func BenchmarkNewV2(b *testing.B) {
	benchxerrors.RunPerMode(b, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = New(benchNewMsg)
		}
	})
}
func BenchmarkErrorfStd(b *testing.B) {
	err := errors.New(benchNewMsg)
	for i := 0; i < b.N; i++ {
		_ = fmt.Errorf(benchErrorfMsg, err)
	}
}

func BenchmarkErrorfPkg(b *testing.B) {
	err := errors.New(benchNewMsg)
	for i := 0; i < b.N; i++ {
		_ = pkgerrors.Wrap(err, benchErrorfMsg)
	}
}

func BenchmarkErrorfXerrors(b *testing.B) {
	err := errors.New(benchNewMsg)
	for i := 0; i < b.N; i++ {
		_ = xerrors.Errorf(benchErrorfMsg, err)
	}
}

func BenchmarkErrorfV2(b *testing.B) {
	err := errors.New(benchNewMsg)
	benchxerrors.RunPerMode(b, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Errorf(benchErrorfMsg, err)
		}
	})
}

func BenchmarkNewErrorfStd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fmt.Errorf(benchErrorfMsg, errors.New(benchNewMsg))
	}
}

func BenchmarkNewErrorfPkg(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = pkgerrors.Wrap(pkgerrors.New(benchNewMsg), benchErrorfMsg)
	}
}

func BenchmarkNewErrorfXerrors(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = xerrors.Errorf(benchErrorfMsg, xerrors.New(benchNewMsg))
	}
}

func BenchmarkNewErrorfV2(b *testing.B) {
	benchxerrors.RunPerMode(b, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Errorf(benchErrorfMsg, New(benchNewMsg))
		}
	})
}

func BenchmarkNewErrorfErrorfStd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = fmt.Errorf(benchErrorfMsg, fmt.Errorf(benchErrorfMsg, errors.New(benchNewMsg)))
	}
}

func BenchmarkNewErrorfErrorfPkg(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = pkgerrors.Wrap(pkgerrors.Wrap(pkgerrors.New(benchNewMsg), benchErrorfMsg), benchErrorfMsg)
	}
}

func BenchmarkNewErrorfErrorfXerrors(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = xerrors.Errorf(benchErrorfMsg, xerrors.Errorf(benchErrorfMsg, xerrors.New(benchNewMsg)))
	}
}

func BenchmarkNewErrorfErrorfV2(b *testing.B) {
	benchxerrors.RunPerMode(b, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = Errorf(benchErrorfMsg, Errorf(benchErrorfMsg, New(benchNewMsg)))
		}
	})
}

func recurse(f func()) {
	testhelpers.Recurse(256, f)
}

func BenchmarkBigStackNewErrorfErrorfStd(b *testing.B) {
	for i := 0; i < b.N; i++ {
		recurse(func() { _ = fmt.Errorf(benchErrorfMsg, fmt.Errorf(benchErrorfMsg, errors.New(benchNewMsg))) })
	}
}

func BenchmarkBigStackNewErrorfErrorfPkg(b *testing.B) {
	for i := 0; i < b.N; i++ {
		recurse(func() { _ = pkgerrors.Wrap(pkgerrors.Wrap(pkgerrors.New(benchNewMsg), benchErrorfMsg), benchErrorfMsg) })
	}
}

func BenchmarkBigStackNewErrorfErrorfXerrors(b *testing.B) {
	for i := 0; i < b.N; i++ {
		recurse(func() { _ = xerrors.Errorf(benchErrorfMsg, xerrors.Errorf(benchErrorfMsg, xerrors.New(benchNewMsg))) })
	}
}

func BenchmarkBigStackNewErrorfErrorfV2(b *testing.B) {
	benchxerrors.RunPerMode(b, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			recurse(func() { _ = Errorf(benchErrorfMsg, Errorf(benchErrorfMsg, New(benchNewMsg))) })
		}
	})
}

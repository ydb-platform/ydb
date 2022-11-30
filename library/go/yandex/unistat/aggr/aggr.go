package aggr

import "a.yandex-team.ru/library/go/yandex/unistat"

// Histogram returns delta histogram aggregation (dhhh).
func Histogram() unistat.Aggregation {
	return unistat.StructuredAggregation{
		AggregationType: unistat.Delta,
		Group:           unistat.Hgram,
		MetaGroup:       unistat.Hgram,
		Rollup:          unistat.Hgram,
	}
}

// AbsoluteHistogram returns absolute histogram aggregation (ahhh).
func AbsoluteHistogram() unistat.Aggregation {
	return unistat.StructuredAggregation{
		AggregationType: unistat.Absolute,
		Group:           unistat.Hgram,
		MetaGroup:       unistat.Hgram,
		Rollup:          unistat.Hgram,
	}
}

// Counter returns counter aggregation (dmmm)
func Counter() unistat.Aggregation {
	return unistat.StructuredAggregation{
		AggregationType: unistat.Delta,
		Group:           unistat.Sum,
		MetaGroup:       unistat.Sum,
		Rollup:          unistat.Sum,
	}
}

// Absolute returns value aggregation (ammm)
func Absolute() unistat.Aggregation {
	return unistat.StructuredAggregation{
		AggregationType: unistat.Absolute,
		Group:           unistat.Sum,
		MetaGroup:       unistat.Sum,
		Rollup:          unistat.Sum,
	}
}

// SummAlias corresponds to _summ suffix
type SummAlias struct{}

func (s SummAlias) Suffix() string {
	return "summ"
}

// SummAlias corresponds to _hgram suffix
type HgramAlias struct{}

func (s HgramAlias) Suffix() string {
	return "hgram"
}

// SummAlias corresponds to _max suffix
type MaxAlias struct{}

func (s MaxAlias) Suffix() string {
	return "max"
}

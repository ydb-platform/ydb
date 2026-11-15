{% if tech %}

### Алгоритмы

* [Оригинальный whitepaper](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf);

Доступны разные модификации алгоритма:

```yql
AdaptiveDistanceHistogram
AdaptiveWeightHistogram
AdaptiveWardHistogram
BlockWeightHistogram
BlockWardHistogram
```

По умолчанию `HISTOGRAM` является синонимом к `AdaptiveWardHistogram`. Обе функции эквивалентны и взаимозаменимы во всех контекстах.

Алгоритмы Distance, Weight и Ward отличаются формулами объединения двух точек в одну:

```c++
TWeightedValue CalcDistanceQuality(const TWeightedValue& left, const TWeightedValue& right) {
    return TWeightedValue(right.first - left.first, left.first);
}

TWeightedValue CalcWeightQuality(const TWeightedValue& left, const TWeightedValue& right) {
    return TWeightedValue(right.second + left.second, left.first);
}

TWeightedValue CalcWardQuality(const TWeightedValue& left, const TWeightedValue& right) {
    const double N1 = left.second;
    const double N2 = right.second;
    const double mu1 = left.first;
    const double mu2 = right.first;
    return TWeightedValue(N1 * N2 / (N1 + N2) * (mu1 - mu2) * (mu1 - mu2), left.first);
}
```

Чем отличается Adaptive и Block:

{% note info %}

Contrary to adaptive histogram, block histogram doesn't rebuild bins after the addition of each point. Instead, it accumulates points and in case the amount of points overflows specified limits, it shrinks all the points at once to produce histogram. Indeed, there exist two limits and two shrinkage operations:

1. FastGreedyShrink is fast but coarse. It is used to shrink from upper limit to intermediate limit (override FastGreedyShrink to set specific behaviour).
2. SlowShrink is slow, but produces finer histogram. It shrinks from the intermediate limit to the actual number of bins in a manner similar to that in adaptive histogram (set CalcQuality in constuctor)

While FastGreedyShrink is used most of the time, SlowShrink is mostly used for histogram finalization

{% endnote %}

{% endif %}

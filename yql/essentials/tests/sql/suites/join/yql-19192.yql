$samples1 = AsList(<|sampl: 1|>,  <|sampl: 2|>);
$sampling_cumulative_prob1 = AsList(<|color: "blue", cum_prob: 3|>);

$cumulative_bounds = SELECT
    LAG(cum_prob) OVER () AS lower_cum_bound
FROM AS_TABLE($sampling_cumulative_prob1);

SELECT
    s.sampl,
    cb.lower_cum_bound
FROM AS_TABLE($samples1) AS s
CROSS JOIN $cumulative_bounds AS cb;
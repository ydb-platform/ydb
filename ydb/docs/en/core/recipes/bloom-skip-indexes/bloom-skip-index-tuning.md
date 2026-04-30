# Bloom skip index tuning

This recipe shows a simple way to tune local Bloom skip index parameters after initial rollout.

## When to tune

If queries still read too much data, try lowering `false_positive_probability`.  
If index size is too high, try raising `false_positive_probability`.

## Example: tuning `bloom_ngram_filter` parameters

```yql
ALTER TABLE `/Root/events` ALTER INDEX idx_msg SET (
    ngram_size = 4,
    false_positive_probability = 0.005,
    case_sensitive = false
);
```

## Practical tips

* Start with `false_positive_probability = 0.01`, then tune based on real read metrics and index size.
* `ngram_size` typically starts at `3`; increasing it can make filtering stricter for longer substrings.
* Change one parameter at a time and compare results under the same workload.

# Subcolumns

## Classes

[`TSubColumnsArray`](accessor.h#L22) is a fully loaded JSON column. It stores separated, frequently used paths in `TColumnsData` as regular column arrays, and the remaining paths in `TOthersData` as a sparse representation.

[`TSubColumnsPartialArray`](partial.h#L44) has metadata for all subcolumns, and actual data for specific separated ones or Others as a whole is loaded on request.

[`TJsonPathAccessor`](json_value_path.h#L51) is an application of JSONPath to column. Its result can be retrieved via `VisitValues` method. It can be in invalid state (no internal array) - for now invalid accessors seem to never be created.

[`TDictStats`](stats.h#L20) describes the stored paths in separated/others. It resolves a requested JSONPath to a `TResolvedPath`, which identifies the stored column, its value type, and the suffix that remains to be evaluated.

## JSON_VALUE resolution

[`TSubColumnsArray::GetPathAccessor()`](accessor.h#L115) resolves the requested JSONPath against the separated and Others stats before constructing an accessor. The source-selection algorithm is [`ResolveBestPath`](stats.h#L316).

1. TDictStats selects the best match among its paths and represents it as a `TResolvedPath`. A path may be matched by partially, for example, path `$.a.b[0]` may be matched by subcolumn `a` with suffix `.b[0]`, or by subcolumn `a.b` with suffix `[0]`. Matches are compared by the length of matched path prefix.
2. Matches between separated and others are compared and the best one is selected, the result is represented as a `TResolvedPathMatch` (`TResolvedPath` + where did it come from).
3. `TColumnsData` constructs a `TJsonPathAccessor` over its stored subcolumn, or `TOthersData` constructs a `TJsonPathAccessor` holding a sparse array for requested key.
4. Resulting values are retrieved via `TJsonPathAccessor::VisitValues()`, which applies the remaining path suffix over its stored array. For example, applying `[0]` to `[[1, 2], [3, 4 ]]` produces `[1, 3]` sequence.

If no stored path matches a valid request, the result is a valid, row-aligned accessor with `recordsCount` NULL values. This preserves SQL `JSON_VALUE` semantics and row cardinality for filters and expressions.

An invalid JSONPath returns a failed `TConclusion`.

For a partial array, `GetPathAccessor()` resolves only among loaded data. A valid path absent from header stats returns the same row-aligned all-NULL accessor as a full array. A path that matches header stats but has not been loaded violates the fetch contract. Fetch planning and accessor lookup must use the same canonical path resolution.

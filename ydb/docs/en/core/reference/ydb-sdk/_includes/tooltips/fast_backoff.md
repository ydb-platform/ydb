**Fast exponential backoff** is one of the backoff strategies used in {{ ydb-short-name }} SDK when retrying queries returned with an error.<br/>
The initial interval for this strategy is several **milliseconds**. For each subsequent attempt, the interval increases exponentially.<br/>
For more information, see [{#T}](../../error_handling.md#handling-retryable-errors).

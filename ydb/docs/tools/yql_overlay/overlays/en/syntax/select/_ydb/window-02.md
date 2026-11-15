You can set an optional *frame definition* (`frame_definition`) one of two ways:

* `ROWS frame_begin`
* `ROWS BETWEEN frame_begin AND frame_end`

*The frame start* (`frame_begin`) and *frame end* (`frame_end`) are set one of the following ways:

* `UNBOUNDED PRECEDING`
* `offset PRECEDING`
* `CURRENT ROW`
* `offset FOLLOWING`
* `UNBOUNDED FOLLOWING`

Here, the *frame* `offset` is a non-negative numeric literal. If the frame end isn't set, the `CURRENT ROW` is assumed.

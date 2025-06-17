# durationpy

> Module for converting between `datetime.timedelta` and Go's Duration strings.

### Install

``` sh
$ pip install durationpy
```


### Parse

* `ns` - nanoseconds
* `us` - microseconds
* `ms` - millisecond
* `s` - second
* `m` - minute
* `h` - hour

``` py
# parse
td = durationpy.from_str("4h3m2s1ms")

# format
durationpy.to_str(td)
```

**Note:** nanosecond precision is lost because `datetime.timedelta` uses microsecond resolution.

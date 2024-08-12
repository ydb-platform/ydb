# MemoryLoad

Allocates memory blocks of the specified size at certain intervals. After the load is removed, the allocated memory is released. Using this actor, you can test the logic, e.g., whether a certain trigger is fired when the [RSS]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Resident_set_size){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Resident_set_size){% endif %} limit is reached.

{% note info %}

This ad-hoc actor is used for testing specific functionality. This is not a load actor. It is designed to check whether something works properly.

{% endnote %}

## Actor parameters {#options}

| Parameter | Description |
--- | ---
| `DurationSeconds` | Load duration in seconds. |
| `BlockSize` | Allocated block size in bytes. |
| `IntervalUs` | Interval between block allocations in microseconds. |

## Examples {#examples}

The following actor allocates blocks of `1048576` bytes every `9000000` microseconds during `3600` seconds and takes up 32 GB while running:

```proto
MemoryLoad: {
    DurationSeconds: 3600
    BlockSize: 1048576
    IntervalUs: 9000000
}
```

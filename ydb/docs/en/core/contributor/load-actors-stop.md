# Stop

Using this command, you can stop either entire load or only the specified part of it.

## Actor parameters {#options}

| Parameter | Description |
--- | ---
| `Tag` | Tag of the load actor to stop. You can view the tag in the cluster Embedded UI. |
| `RemoveAllTags` | If this parameter value is set to `True`, all the load actors are stopped. |

## Examples {#examples}

The command below stops the load tagged `123`:

```proto
Stop: {
    Tag: 123
}
```

To stop the entire load, run this command:

```proto
Stop: {
    RemoveAllTags: true
}
```

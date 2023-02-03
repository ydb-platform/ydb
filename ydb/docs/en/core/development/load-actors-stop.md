# Stop

Using this actor, you can stop either all or only the specified actors.

## Actor specification {#proto}

```proto
message TStop {
    optional uint64 Tag = 1;
    optional bool RemoveAllTags = 2;
}
```

## Actor parameters {#options}

| Parameter | Description |
--- | ---
| `Tag` | Load tag that can be set to stop a specific load. You can view the tag in the Embedded UI.<br>Type: `uint64`. |
| `RemoveAllTags` | Stop all load actors on the node.<br>Type: `bool`. |

## Examples {#examples}

{% list tabs %}

- CLI

   Actor specification to be used to stop the load tagged `123` on the node with ID `1`:

   ```proto
   NodeId: 1
   Event: {
       Stop: {
           Tag: 123
       }
   }
   ```

   Actor specification to be used to stop all the load on the node with ID `1`:

   ```proto
   NodeId: 1
   Event: {
       Stop: {
           RemoveAllTags: true
       }
   }
   ```

{% endlist %}

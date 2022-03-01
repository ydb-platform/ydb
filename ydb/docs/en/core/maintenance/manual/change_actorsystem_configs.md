# Changing an actor system's configuration

## On static nodes

Static nodes take the configuration of the actor system from the kikimr/cfg/sys.txt file.

After changing the configuration, restart the node.

## On dynamic nodes

Dynamic nodes take the configuration from the CMS. To change it, you can use the following command.

```proto
ConfigureRequest {
  Actions {
    AddConfigItem {
      ConfigItem {
        // UsageScope: { ... }
        Config {
          ActorSystemConfig {
            <actor system config>
          }  
        }
        MergeStrategy: 3
      }
    }
  }
}
```

kikimr -s <endpoint> admin console execute --domain=<domain> --retry=10 actorsystem.txt

```





# Изменение конфигурации актор-системы

## На статических узлах

Статические ноды берут конфигурацию актор-системы из файла kikimr/cfg/sys.txt.

После замены конфигурации требуется перезапустить узел.

## На динамических узлах

Динамические ноды берут конфигурацию из cms, чтобы изменить ее можно воспользоваться следующей командой.

```proto
ConfigureRequest {
  Actions {
    AddConfigItem {
      ConfigItem {
        // UsageScope: { ... }
        Config {
          ActorSystemConfig {
            <конфиг актор-системы>
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

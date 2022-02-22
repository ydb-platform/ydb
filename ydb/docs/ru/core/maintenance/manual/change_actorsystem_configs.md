
# Изменение конфигурации акторсистемы

## На статических нода

Статические ноды берут конфигурацию акторсистемы из файла расположенного kikimr/cfg/sys.txt.

После замены конфигурации требуется перезапустить ноду.

## На динамических нодых

Динамические ноды берут конфигурацию из cms, чтобы изменить ее можно воспользоаться следуюшей командой.

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
kikimr -s <ендпоинт> admin console execute --domain=<domain> --retry=10 actorsystem.txt
```

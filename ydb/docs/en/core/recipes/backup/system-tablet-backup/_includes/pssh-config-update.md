Copy the updated configuration to a temporary file on each host:

```bash
pscp -h hosts.txt config.yaml /tmp/config.yaml.new
```

Make a backup of the current configuration, replace it with the new version, and protect it from accidental changes:

```bash
pssh -h hosts.txt 'sudo cp /opt/ydb/cfg/config.yaml /opt/ydb/cfg/config.yaml.bak && sudo mv /tmp/config.yaml.new /opt/ydb/cfg/config.yaml && sudo chattr +i /opt/ydb/cfg/config.yaml'
```

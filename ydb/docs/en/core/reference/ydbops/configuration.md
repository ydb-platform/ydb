## Configuring ydbops

{% note info %}

The article is being updated. Expect new content to appear and minor fixes to existing content.

{% endnote %}


`ydbops` can be run, specifying all the necessary command line arguments in the command invocation. But if you find yourself executing `ydbops` often, it might be simpler to take advantage of: 

- config file feature
- environment variables

### Config file

Configuration file for `ydbops` is a yaml-formatted file containing multiple profiles. Profiles for `ydbops` work in the same way as profiles in [{{ ydb-short-name }} CLI do](../ydb-cli/profile/index.md).

Certain command line options can be written in the configuration file, instead of specifying them directly in the `ydbops` invocation. 

Calling the `ydbops restart` command without a profile:

```
ydbops restart \
 -e grpc://some.host.in.some.domain:2135 \
 --kubeconfig ~/.kube/config \
 --k8s-namespace your-k8s-namespace \
 --user admin \
 --password-file ~/my-password-file \
 --tenant --tenant-list=my-tenant
```

Calling the same `ydbops restart` command with profile options enabled makes the command much shorter:

```
ydbops restart \
 --config-file ./config.yaml \
 --tenant --tenant-list=my-tenant
```

### Profile management commands

Currently `ydbops` does not support creation, modification and activation of profiles through CLI [the way that {{ ydb-short-name }} CLI does it](../ydb-cli/profile/index.md#commands).

Please create a configuration file manually. 

### Configuration file reference

Here is an example of configuration file with all possible options that can be specified and example values (you will most likely not need all of these options at the same time):

```
# a special key `current-profile` can be specified to 
# avoid specifying the active profile in the CLI invocation
current-profile: my-profile

my-profile:
  endpoint: grpc://your.ydb.cluster.fqdn:2135

  # if using a custom CA for TLS, CA file can be specified:
  ca-file: /path/to/custom/ca/file

  # when using static credentials, you can specify username and password file:
  user: your-ydb-user-name
  password-file: /path/to/password-file

  # when using access
  token-file: /path/to/ydb/token

  # if working with YDB clusters in Kubernetes, Kubernetes-specific options can be specified:
  kubeconfig: /path/to/kube/config
  k8s-namespace: your-default-k8s-namespace
```
## Environment variables

There is also an option to specify several environment variables instead of passing command line arguments.

For an explanation which options take precedence, please invoke `ydbops --help`.

- `$YDB_TOKEN`, can be passed instead of `--token-file` flag or `token-file` profile option.
- `$YDB_PASSWORD`, can be passed instead of `--password-file` flag or `password-file` profile option.
- `$YDB_USER`, can be passed instead of `--user` flag or `user` profile option.

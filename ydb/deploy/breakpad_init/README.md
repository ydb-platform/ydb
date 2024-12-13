# Breakpad Init Library

## Build

```bash
ya package --docker --docker-build-arg=BREAKPAD_GIT_TAG="v2023.06.01" ydb/deploy/breakpad_init/pkg.json
```

### Extract file

```bash
id=$(docker create cr.yandex/crp2lrlsrs36odlvd8dv/breakpad_init:<branch>.<revision> sleep infinity)
docker cp $id:/usr/lib/libbreakpad_init.so libbreakpad_init.so
docker rm -v $id
```

## Usage

1. Copy library to path `/usr/lib/libbreakpad_init.so`

2. Set suid bit on library to execute `chmod 4644 /usr/lib/libbreakpad_init.so`

3. Set environment variable with basename `export LD_PRELOAD=libbreakpad_init.so`

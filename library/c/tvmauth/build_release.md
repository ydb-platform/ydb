Steps:
1. `dch -i` in debians/ + commit
2. Run task: https://sandbox.yandex-team.ru/scheduler/43549/view
3. `ssh dupload.dist.yandex.net`
4. `for r in bionic xenial precise trusty focal; do sudo dmove yandex-$r stable libtvmauth-dev <version> unstable ; done`
5. Push package in Conductor OR `for r in bionic xenial precise trusty focal; do sudo dmove yandex-$r stable libtvmauth <version> unstable ; done`

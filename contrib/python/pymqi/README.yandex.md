Yandex Arcadia note
-------------------
1. Сейчас библиотека собирается только под linux
2. Для работы библиотеки понадобятся deb-пакеты IBM MQ
   - ibmmq-client
   - ibmmq-runtime
   - ibmmq-sdk
3. Деб пакеты можно поставить так
```shell script
mkdir /ibm-mq-client && cd /ibm-mq-client \
    && wget -q -O IBM-MQC_dist.tar.gz https://proxy.sandbox.yandex-team.ru/795152415 \
    && tar -xzf IBM-MQC_dist.tar.gz \
    && ./mqlicense.sh -accept \
    && dpkg -i ibmmq-runtime_9.1.1.0_amd64.deb \
    && dpkg -i ibmmq-sdk_9.1.1.0_amd64.deb ibmmq-client_9.1.1.0_amd64.deb
```

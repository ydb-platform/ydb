{% note info %}

При использовании консольных утилит Kafka с Java 23 и получении ошибки
`java.lang.UnsupportedOperationException: getSubject is supported only if a security manager is allowed`
, либо запустите команду, используя другую версию Java ([как сменить версию Java на macos](https://stackoverflow.com/questions/21964709/how-to-set-or-change-the-default-java-jdk-version-on-macos))
, либо запустите команду, указав для java флаг `-Djava.security.manager=allow`.
Например: `KAFKA_OPTS=-Djava.security.manager=allow kafka-topics --boostratp-servers localhost:9092 --list`

{% endnote %}
### О релизе {{ ydb-short-name }} v23.1 {#2023-webinar-ydb-23-1}

{% include notitle [releases_tag](../../tags.md#releases) %}

В вебинаре рассказывается о поддержке изначального сканирования в CDC, атомарной замене индексов, и аудитном логе:
* Устройство [Change Data Capture](https://www.youtube.com/live/vzKoEVvESi0?si=PDu8VliKHNKn25iE&t=171) (механизм отслеживания изменения данных в таблицах) и улучшения этой системы в новом релизе. [{{ team.nizametdinov.name }}]({{ team.nizametdinov.profile }}) ({{ team.nizametdinov.position }}).
* Новая функциональность YDB – [аудитный лог](https://www.youtube.com/live/vzKoEVvESi0?si=umC_WpfI8XXpWrRY&t=925), которая позволяет отслеживать ключевые действия и события в системе. [{{ team.rykov.name }}]({{ team.rykov.profile }}) ({{ team.rykov.position }}).
* Улучшение в конфигурации [акторной системы]((https://www.youtube.com/live/vzKoEVvESi0?si=roXublyzdBy8UNjC&t=1177)). [{{ team.kriukov.name }}]({{ team.kriukov.profile }}) ({{ team.kriukov.position }})
* Улучшения в форматах [передачи данных](https://www.youtube.com/watch?v=vzKoEVvESi0&t=1381s) между этапами выполнения запросов. [{{ team.gridnev.name }}]({{ team.gridnev.profile }}) ({{ team.gridnev.position }}).
* [Оптимизация производительности YDB](https://www.youtube.com/live/vzKoEVvESi0?si=vLEerc2xz9O9LABz&t=1896) – кеширование паттернов графов вычислений. [{{ team.kuznetcov.name }}]({{ team.kuznetcov.profile }}) ({{ team.kuznetcov.position }}).
* [Атомарное переименование](https://www.youtube.com/watch?v=vzKoEVvESi0&t=2122s) вторичных индексов. [{{ team.cherednik.name }}]({{ team.cherednik.profile }}) ({{ team.cherednik.position }}).
* [Поддержка вторичных индексов](https://www.youtube.com/watch?v=vzKoEVvESi0&t=2318s) в сканирующих запросах. [{{ team.sidorina.name }}]({{ team.sidorina.profile }}) ({{ team.sidorina.position }}).
* Улучшения в переносе [предикатов на чтения](https://www.youtube.com/watch?v=vzKoEVvESi0&t=2454s) из таблиц. {{ team.surin.name }}.

@[YouTube](https://www.youtube.com/watch?v=vzKoEVvESi0&t=69s)

[Слайды](https://presentations.ydb.tech/2023/ru/release_webinar_v23.1/presentation.pdf)

### Используем {{ ydb-short-name }}: Возможности встроенного веб-интерфейса и CLI {#2023-webinar-ydb-interface}

{% include notitle [practice_tag](../../tags.md#practice) %}


[{{ team.kovalenko.name }}]({{ team.kovalenko.profile }}) ({{ team.kovalenko.position }}) рассказал и показал как запустить нагрузку на кластер с помощью {{ ydb-short-name }} CLI, как найти причины проблем с производительностью запросов, и как с помощью встроенного веб-интерфейса оценить состояние кластера.
@[YouTube](https://www.youtube.com/watch?v=jB8RBnA4Y-Y)

Вебинар будет полезен тем, кто работает с {{ ydb-short-name }}, в том числе и разработчикам приложений.

### Развертывание {{ ydb-short-name }} в Kubernetes {#2023-webinar-ydb-kubernetes}

{% include notitle [practice_tag](../../tags.md#practice) %}

[{{ team.fomichev.name }}]({{ team.fomichev.profile }}) ({{ team.fomichev.position }}), [{{ team.babich.name }}]({{ team.babich.profile }}) ({{ team.babich.position }}) и [{{ team.gorbunov.name }}]({{ team.gorbunov.profile }}) ({{ team.gorbunov.position }}) рассказали про архитектуру системы и объяснили, как гарантируется надёжное хранение данных. Также спикеры продемонстрировали, как быстро развернуть собственный кластер в Managed Kubernetes® в Yandex Cloud и рассказали о практических рекомендациях по конфигурации узлов.

@[YouTube](https://www.youtube.com/watch?v=qzcB5OQaiYY)

Вебинар будет полезен SRE-инженерам и разработчикам, которые занимаются развёртыванием и администрированием {{ ydb-short-name }} с помощью Kubernetes® — как в публичных, так и в частных облаках.

### Анализ потоковых данных с помощью Yandex Query - интерактивного сервиса виртуализации данных в {{ ydb-short-name }} {#2023-webinar-ydb-data-streams}
[{{ team.dmitriev.name }}]({{ team.dmitriev.profile }}) ({{ team.dmitriev.position }}) рассказал, что такое обработка потоковых данных, с какими задачами она помогает справляться и как её выполнять с помощью Yandex Query. В практической части вебинара показаны решения нескольких задач с помощью потоковой аналитики в сервисе Yandex Query.

@[YouTube](https://www.youtube.com/watch?v=PW7v57ELCfQ)

Вебинар будет полезен разработчикам, особенно тем, кто ещё не пользовался потоковой обработкой и хочет разобраться, что это такое.
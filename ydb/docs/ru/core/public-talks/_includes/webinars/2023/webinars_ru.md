### Вебинар о релизе YDB v23.1
В вебинаре рассказывается о поддержке изначального сканирования в CDC, атомарной замене индексов, и аудитном логе: 
* Устройство [Change Data Capture](https://www.youtube.com/live/vzKoEVvESi0?si=PDu8VliKHNKn25iE&t=171) (механизм отслеживания изменения данных в таблицах) и улучшения этой системы в новом релизе. [Ильназ Низаметдинов](https://www.linkedin.com/in/nilnaz/); 
* Новая функциональность YDB – [аудитный лог](https://www.youtube.com/live/vzKoEVvESi0?si=umC_WpfI8XXpWrRY&t=925), которая позволяет отслеживать ключевые действия и события в системе. [Андрей Рыков](https://www.linkedin.com/in/andrei-rykov-5936b4222/);
* Улучшение в конфигурации [акторной системы]((https://www.youtube.com/live/vzKoEVvESi0?si=roXublyzdBy8UNjC&t=1177)). [Александр Крюков](https://www.linkedin.com/in/kruall/)
* Улучшения в форматах [передачи данных](https://www.youtube.com/watch?v=vzKoEVvESi0&t=1381s) между этапами выполнения запросов. [Виталий Гриднев](https://www.linkedin.com/in/gridnevvvit/);
* [Оптимизация производительности YDB](https://www.youtube.com/live/vzKoEVvESi0?si=vLEerc2xz9O9LABz&t=1896) – кеширование паттернов графов вычислений. [Владислав Кузнецов](https://www.linkedin.com/in/vlad-kuznetcov-a8a012276/);
* [Атомарное переименование](https://www.youtube.com/watch?v=vzKoEVvESi0&t=2122s) вторичных индексов. [Даниил Чередник](https://www.linkedin.com/in/daniil-c-1110165b/);
* [Поддержка вторичных индексов](https://www.youtube.com/watch?v=vzKoEVvESi0&t=2318s) в сканирующих запросах. [Юлия Сидорина](https://www.linkedin.com/in/yuliya-sidorina-a17ab6220/);
* Улучшения в переносе [предикатов на чтения](https://www.youtube.com/watch?v=vzKoEVvESi0&t=2454s) из таблиц. [Михаил Сурин](https://github.com/ydb-platform/ydb-presentations/blob/main/2023/ru/release_webinar_v23.1/404).

@[YouTube](https://www.youtube.com/watch?v=vzKoEVvESi0&t=69s)

### Используем YDB: Возможности встроенного веб-интерфейса и CLI
[Антон Коваленко](https://www.linkedin.com/in/kovalad/) рассказал и показал как запустить нагрузку на кластер с помощью YDB CLI, как найти причины проблем с производительностью запросов, и как с помощью встроенного веб-интерфейса оценить состояние кластера.
@[YouTube](https://www.youtube.com/watch?v=jB8RBnA4Y-Y)

### Развертывание YDB в Kubernetes
[Андрей Фомичев](https://www.linkedin.com/in/andrey-fomichev), [Михаил Бабич](https://www.linkedin.com/in/mikhail-babich-807700270/) и [Максим Горбунов](https://www.linkedin.com/in/maksim-gorbunov-20283a55/) рассказали про архитектуру системы и объяснили, как гарантируется надёжное хранение данных. Также спикеры продемонстрировали, как быстро развернуть собственный кластер в Managed Kubernetes® в Yandex Cloud и рассказали о практических рекомендациях по конфигурации узлов.

@[YouTube](https://www.youtube.com/watch?v=qzcB5OQaiYY)

Вебинар будет полезен SRE-инженерам и разработчикам, которые занимаются развёртыванием и администрированием YDB с помощью Kubernetes® — как в публичных, так и в частных облаках.

### Анализ потоковых данных с помощью Yandex Query - интерактивного сервиса виртуализации данных в YDB
[Алексей Дмитриев](https://www.linkedin.com/in/алексей-дмитриев-7914b535/) рассказал, что такое обработка потоковых данных, с какими задачами она помогает справляться и как её выполнять с помощью Yandex Query. В практической части вебинара показаны решения нескольких задач с помощью потоковой аналитики в сервисе Yandex Query.

@[YouTube](https://www.youtube.com/watch?v=PW7v57ELCfQ)

Вебинар будет полезен разработчикам, особенно тем, кто ещё не пользовался потоковой обработкой и хочет разобраться, что это такое.
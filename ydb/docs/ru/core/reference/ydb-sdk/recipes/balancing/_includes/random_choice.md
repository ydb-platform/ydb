# Равномерный случайный выбор

{% include [work in progress message](../../_includes/addition.md) %}

`Ydb SDK` использует алгоритм `random_choice` (равномерную случайную балансировку) по умолчанию.

Ниже приведены примеры кода принудительной установки алгоритма балансировки "равномерный случайный выбор" в разных `Ydb SDK`

{% list tabs %}

- Go


  {% include [go.md](random_choice/go.md) %}


{% endlist %}

# Использование таймаутов

В разделе приведено описание доступных таймаутов и представлены примеры использования на различных языках программирования.

## Предпосылки к использованию таймаутов {#intro}

Механизм таймаутов в {{ ydb-short-name }} призван решить следующие проблемы:

* Не дать запросу выполнятся так долго, что результат запроса становится не интересен для дальнейшего использования.
* Обнаружить проблемы сетевой связности.

Оба этих случая важны для обеспечения отказоустойчивости системы в целом. Рассмотрим таймауты подробнее.

## Таймаут на операцию {#operational}

Значение ``operation_timeout`` определяет время, в течение которого результат запроса интересен пользователю. Если за данное время операция не выполнилась, сервер возвращает ошибку c кодом ``Timeout`` и попытается прекратить выполнение запроса, однако отмена запроса не гарантируется. Таким образом, запрос, на который пользователю была возвращена ошибка ``Timeout``, может быть как успешно выполнен на сервере, так и отменен.

## Таймаут отмены операции {#cancel}

Значение ``cancel_after`` определяет время, через которое сервер начнет отмену запроса, если отменить запрос возможно. В случае успешной отмены запроса сервер вернет ошибку с кодом ``Cancelled``.

## Транспортный таймаут {#transport}

На каждый запрос клиент должен выставить транспортный таймаут. Данное значение позволяет определить количество времени, которое клиент готов ждать ответа от сервера. Если за данное время сервер не ответил, то клиенту будет возвращена транспортная ошибка c кодом ``DeadlineExceeded``. Важно выставить такое значение клиентского таймаута чтоб при нормальной работе приложения и сети транспортные таймауты не срабатывали.

## Применение таймаутов {#usage}

Всегда рекомендуется устанавливать и таймаут на операцию и транспортный таймаут. Значение транспортного таймаута следует делать на 50-100 миллисекунд больше чем значение таймаута на операцию, чтобы оставался некоторый запас времени, за который клиент сможет получить серверную ошибку c кодом ``Timeout``.

Пример использования таймаутов:

{% list tabs %}

- Python

  ```python
  import ydb

  def execute_in_tx(session, query):
    settings = ydb.BaseRequestSettings()
    settings = settings.with_timeout(0.5)  # transport timeout
    settings = settings.with_operation_timeout(0.4)  # operation timeout
    settings = settings.with_cancel_after(0.4)  # cancel after timeout
    session.transaction().execute(
        query,
        commit_tx=True,
        settings=settings,
    )
  ```

{% if oss == true %}

- C++

  ```cpp
  #include <ydb/public/sdk/cpp/client/ydb.h>
  #include <ydb/public/sdk/cpp/client/ydb_table.h>
  #include <ydb/public/sdk/cpp/client/ydb_value.h>

  using namespace NYdb;
  using namespace NYdb::NTable;

  TAsyncStatus ExecuteInTx(TSession& session, TString query, TParams params) {
    return session.ExecuteDataQuery(
        query
        , TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()
        , TExecDataQuerySettings()
        .OperationTimeout(TDuration::MilliSeconds(300))  // operation timeout
        .ClientTimeout(TDuration::MilliSeconds(400))   // transport timeout
        .CancelAfter(TDuration::MilliSeconds(300)));  // cancel after timeout
  }

  ```

{% endif %}

- Go

  ```go
  import (
    "context"

    ydb "github.com/ydb-platform/ydb-go-sdk/v3"
    "github.com/ydb-platform/ydb-go-sdk/v3/table"
  )

  func executeInTx(ctx context.Context, s table.Session, query string) {
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*300) // client and by default operation timeout
	defer cancel()
	ctx = ydb.WithOperationTimeout(ctx, time.Millisecond*400)     // operation timeout override
	ctx = ydb.WithOperationCancelAfter(ctx, time.Millisecond*300) // cancel after timeout
	tx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())
	_, res, err := s.Execute(ctx, tx, query, table.NewQueryParameters())
  }
  ```

{% endlist %}
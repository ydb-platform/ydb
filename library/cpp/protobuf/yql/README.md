Функции данной библиотеки, позволяют генерировать метаинформацию, необходимую YQL для того, чтобы работать с содержимым protobuf-сообщения как со структурой.

Пример задания атрибута для таблицы в YT:
```c++
#include <proto/person.pb.h>
#include <library/cpp/protobuf/yql/descriptor.h>

auto client = NYT::CreateClient(ServerName);
client->Create(OutputPath, NT_TABLE,
    TCreateOptions().Attributes(NYT::TNode()
        ("_yql_proto_field_Data", GenerateProtobufTypeConfig<TPersonProto>())
);
```
Далее, в YQL можно будет написать следующий запрос:
```sql
SELECT t.Data.Name, t.Data.Age,  FROM [table] AS t;
```

Для того, чтобы работать с protobuf-сообщениями, сохранёнными в разных представлениях есть специальные опции. На данный момент поддерживается три формата представления данных - binary-protobuf, text-protbuf и json.

Пример использования опций:
```c++
GenerateProtobufTypeConfig<TPersonProto>(
    TProtoTypeConfigOptions().SetProtoFormat(PF_PROTOTEXT))
);
```

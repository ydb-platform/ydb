## Чтение и обработка данных

Записи потока изменений доступны для чтения по протоколу персистентных очередей. Ниже приведён фрагмент кода:

```c++
// Create client
TPersQueueClient client(driver, TPersQueueClientSettings().Database("/path/to/database"));

// Add consumer
client.AddReadRule("/path/to/table/feed_name", TAddReadRuleSettings()
    .ReadRule(TReadRuleSettings().ConsumerName("consumer_name")));

// Create read session
auto reader = client.CreateReadSession(TReadSessionSettings()
    .AppendTopics(TString("/path/to/table/feed_name"))
    .ConsumerName("consumer_name")
);

// Read records
while (true) {
    auto ev = reader->GetEvent(true);
    if (auto* data = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*ev)) {
        for (const auto& item : data->GetMessages()) {
            // Process record
        }
    } else if (auto* create = std::get_if<TReadSessionEvent::TCreatePartitionStreamEvent>(&*ev)) {
        create->Confirm();
    } else if (auto* destroy = std::get_if<TReadSessionEvent::TDestroyPartitionStreamEvent>(&*ev)) {
        destroy->Confirm();
    } else if (std::get_if<TSessionClosedEvent>(&*ev)) {
        break;
    }
}
```

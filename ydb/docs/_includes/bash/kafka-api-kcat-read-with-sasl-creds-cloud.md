```bash
kcat -C \
  -b <kafka_api_endpoint> \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=PLAIN \
  -X sasl.username="@<path_to_database>" \
  -X sasl.password="<api_key>" \
  -X check.crcs=false \
  -X partition.assignment.strategy=roundrobin \
  -G <consumer-name> <topic-name>
```
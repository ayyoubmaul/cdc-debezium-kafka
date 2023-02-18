# Goal

Goal of this project is to stream change data capture from MySQL then load it to Postgres as Data Warehouse, using Debezium as a source and sink connector between Kafka and MySQL/Postgres


```
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @conf/source.json
```

```
cd lib && docker cp ./ cdc-debezium-kafka_connect_1:/kafka/libs
```

```
curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @conf/sink.json
```

restart debezium connect container

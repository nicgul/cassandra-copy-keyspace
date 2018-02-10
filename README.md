# cassandra-copy-keyspace
Copies a Cassandra keyspace to a new named keyspace. Any use at your own risk.

Build:
```
mvn install
```

Usage:
```
command:
java -jar target/cassandra-copy-keyspace-1.0-SNAPSHOT-jar-with-dependencies.jar

mandatory parameters:
source=source-keyspace
target=target-keyspace

optional parameters
[sourceHost=host[:port] [targetHost=host[:port]]
```

Example:
```
java -jar target/cassandra-copy-keyspace-1.0-SNAPSHOT-jar-with-dependencies.jar sourceHost=localhost:9042 targetHost=localhost:9042 source=test target=test_copy
```

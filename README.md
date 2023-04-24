# About

Recreate memory leak claimed by [reactor-kafka #341](https://github.com/reactor/reactor-kafka/issues/341).

# Usage

```shell

mvn package
java -Xmx300m -jar target/reactor-kafka-341-1.0-SNAPSHOT.jar
```

## How to run this project

### You can run this project inside the pyspark container.

```sh
docker run -d -t --name pyspark --user root --rm -v <host path>:<container path> -p 4040:4040 apache/spark-py /opt/spark/bin/pyspark
```
> To debug and run inside pyspark container you need to run this container as privileged mode

This command will attach a python shell with everything setup in order to run Apache Spark Python API


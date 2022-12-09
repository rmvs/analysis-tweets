## How to run this project

### This command will start a pyspark container and attach a python shell with everything setup in order to run this project

```sh
docker run -d -t --name pyspark --user root --rm -v <host path>:<container path> -p 4040:4040 apache/spark-py /opt/spark/bin/pyspark
```
> To debug and run inside pyspark container you need to run this container as privileged mode

# Flume agent with Hadoop client libraries.
# Based on the bde2020 namenode image so that the Flume HDFS sink
# can talk to our HDFS cluster (the stock bde2020/flume image has no Hadoop client).
FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

ENV FLUME_HOME=/opt/apache-flume-1.9.0-bin

RUN curl -fSL https://archive.apache.org/dist/flume/1.9.0/apache-flume-1.9.0-bin.tar.gz -o /tmp/flume.tgz \
    && tar -xzf /tmp/flume.tgz -C /opt \
    && rm -f /tmp/flume.tgz \
    # Flume 1.9 bundles guava-11, which is too old for the Hadoop 3 client.
    # Remove it so the newer guava from Hadoop's classpath is used.
    && mv /opt/apache-flume-1.9.0-bin/lib/guava-11.0.2.jar /tmp/

ENV PATH=$FLUME_HOME/bin:$PATH

COPY run-agent.sh /run-agent.sh
RUN chmod +x /run-agent.sh
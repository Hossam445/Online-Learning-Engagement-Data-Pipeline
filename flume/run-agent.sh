#!/bin/bash
# Flume agent startup script.
# Runs inside the flume container after the Hadoop entrypoint has
# generated the Hadoop configs and waited for the namenode/datanode.

set -e

# Give Flume's HDFS sink access to the Hadoop client libraries.
export FLUME_CLASSPATH=$(/opt/hadoop-3.2.1/bin/hadoop classpath)

# Drop the raw CSV into the spooling directory (skipping the header row).
mkdir -p /flume-spool
tail -n +2 /data/online_learning_engagement_dataset.csv > /flume-spool/online_learning_engagement_dataset.csv

# Start the Flume agent (Source -> Channel -> HDFS Sink).
exec flume-ng agent \
  --conf /opt/apache-flume-1.9.0-bin/conf \
  --conf-file /flume-conf/flume.conf \
  --name agent \
  -Dflume.root.logger=INFO,console
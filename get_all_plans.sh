#!/bin/bash

cd $1;

find query12.sql | while read line; do
    for i in {0..0}
    do
    query=$(basename "$line" ".sql")
    rm -rf /opt/output/$query/$i
    mkdir -p /opt/output/$query/$i
    echo $query
    (hive --hiveconf hive.execution.engine=tez --hiveconf hive.stats.fetch.column.stats=true \
        --hiveconf hive.session.id="$query.$i" \
        --hiveconf hive.log.explain.output=true \
        --hiveconf hive.log.dir="/opt/output/$query/$i" \
        --hiveconf hive.log.file="$query.log" \
        --hiveconf hive.session.history.enabled=true \
        --hiveconf hive.querylog.location="/opt/output/$query/$i" \
        --hiveconf hive.qoop.dumpdir="/opt/output/$query/$i" \
        --hiveconf hive.qoop.fileid="$query" \
        --hiveconf hive.log.level=INFO \
        --hiveconf hive.qoop.combination=$i \
        --hiveconf hive.tez.exec.print.summary=true \
        --hiveconf hive.auto.convert.join.noconditionaltask=false \
        --hiveconf hive.auto.convert.join=false \
        --hiveconf hive.reorder.nway.joins=false \
        -f $line \
        --database tpcds_text_50) 1>/opt/output/$query/$i/$query.out 2>/opt/output/$query/$i/$query.err

    appId=$(cat /opt/output/$query/$i/$query.log |\
        awk '/Submitted application/ {print $NF}' |\
        awk -F')' '{print $1}' | awk -F'_' '{print $2"_"$3}')
    sleep 4 
    hdfs dfs -copyToLocal /tmp/tez-history/history.txt.appattempt\_$appId\_000001 /opt/output/$query/$i/$query.tez
    sleep 6
    yarn logs -applicationId application\_$appId 1>/opt/output/$query/$i/$query.yarn

    echo /tmp/tez-history/history.txt.appattempt\_$appId\_000001 > /opt/output/$query/$i/$query.debug
    echo application\_$appId >> /opt/output/$query/$i/$query.debug

    x=$(awk '/digraph/ {print FNR}' /opt/output/$query/$i/$query.yarn)
    y=$(awk '/End of LogType:dag/ {print FNR}' /opt/output/$query/$i/$query.yarn)
    y=`expr $y - 1`
    sed -n ${x},${y}p /opt/output/$query/$i/$query.yarn > /opt/output/$query/$i/$query.dot
    # hadoop dfs -copyToLocal /tmp/hadoop-yarn/staging/history/done/2017/04/12/000000/*.jhist \
    #    /opt/output/$query/$i/
    # hadoop dfs -rm -r /tmp/hadoop-yarn/staging/history/done/2017/04/12/000000
    done
done

cd -;

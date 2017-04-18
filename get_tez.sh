#!/bin/bash

cd $1;

find query25.sql | while read line; do
    for i in {0..13}
    do
    query=$(basename "$line" ".sql")

    appId=$(cat /opt/output/tez-with-stats/$query/$i/$query.log |\
        awk '/Submitted application/ {print $NF}' |\
        awk -F')' '{print $1}' | awk -F'_' '{print $2"_"$3}')
    rm /opt/output/tez-with-stats/$query/$i/$query.tez
    hdfs dfs -copyToLocal /tmp/tez-history/history.txt.appattempt\_$appId\_000001 \
        /opt/output/tez-with-stats/$query/$i/$query.tez
    sleep 5 
    # yarn logs -applicationId application\_$appId 1>/opt/output/$query/$i/$query.yarn

    # echo /tmp/tez-history/history.txt.appattempt\_$appId\_000001 > /opt/output/$query/$i/$query.debug
    # echo application\_$appId >> /opt/output/$query/$i/$query.debug

    # x=$(awk '/digraph/ {print FNR}' /opt/output/$query/$i/$query.yarn)
    # y=$(awk '/End of LogType:dag/ {print FNR}' /opt/output/$query/$i/$query.yarn)
    # y=`expr $y - 1`
    # sed -n ${x},${y}p /opt/output/$query/$i/$query.yarn > /opt/output/$query/$i/$query.dot
    # hadoop dfs -copyToLocal /tmp/hadoop-yarn/staging/history/done/2017/04/12/000000/*.jhist \
    #    /opt/output/$query/$i/
    # hadoop dfs -rm -r /tmp/hadoop-yarn/staging/history/done/2017/04/12/000000
    done
done

cd -;

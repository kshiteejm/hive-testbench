#!/bin/bash

for i in {0..4}
do
    ./run_a_plan.sh $1 $i &
done

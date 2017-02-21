#!/bin/bash

parallelism=4
counter=0

for f in $(ls *pcap | sort); do
  if [ "$counter" -lt "$parallelism" ]; then
    echo "Converting $f"
    (time tshark -r $f -Y mysql.query -Y mysql.command -e tcp.stream \
      -e frame.time_epoch -e mysql.command -e mysql.query \
      -Tfields -E quote=d > $f.dat) &
    counter=$((counter+1))
  else
    counter=0
    echo "Waiting for children to finish"
    wait
  fi
done

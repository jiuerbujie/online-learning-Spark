#!/bin/bash

#Usage: %s date_file_name [port] [sendLines/s]
echo '-------------------- we do -----------------------'

LINES_PER_SECOND=1000
SERVER_PORT=9999
echo "we send $LINES_PER_SECOND per second,listening port:$SERVER_PORT"
./movieLensDataSend stream_10k_movielens $SERVER_PORT $LINES_PER_SECOND

echo "------------------ we do end -----------------------"

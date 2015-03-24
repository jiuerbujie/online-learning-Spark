CFLAGS = -Wall #-lpthread
LDFLAGS = -g
CC = gcc
all : movieLensDataSend
movieLensDataSend : movieLensDataSend.c
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm *.o movieLensDataSend


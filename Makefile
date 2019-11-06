CC=gcc
CFLAGS=-I. -g -D_FILE_OFFSET_BITS=64 -I/usr/include/fuse
LIBS = -luuid -lfuse -pthread -lm
DEPS = myfs.h unqlite.h
OBJ = unqlite.o

TARGET1 = myfs

all: $(TARGET1) 

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

$(TARGET1): $(TARGET1).o $(OBJ)
	gcc -o $@ $^ $(CFLAGS) $(LIBS)

.PHONY: clean

clean:
	rm -f *.o *~ core myfs.db myfs.log $(TARGET1)


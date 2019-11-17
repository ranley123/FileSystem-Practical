#include <uuid/uuid.h>
#include <unqlite.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <fuse.h>

#define MAX_FILENAME_LENGTH 256
#define BLOCK_SIZE 4096 // 4 pages
#define MAX_UUIDS_PER_BLOCK (BLOCK_SIZE / sizeof(uuid_t))
#define NUMBER_DIRECT_BLOCKS 12
#define MAX_INDEX_LEVEL 1
#define MAX_FILE_SIZE (BLOCK_SIZE * (NUMBER_DIRECT_BLOCKS + MAX_UUIDS_PER_BLOCK))

#define max(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _a : _b; })
#define min(a,b) \
   ({ __typeof__ (a) _a = (a); \
       __typeof__ (b) _b = (b); \
     _a > _b ? _b : _a; })

typedef struct _fcb
{
    uid_t uid;   // user id
    gid_t gid;   // group id
    mode_t mode; // file mode
    off_t size;  // file size
    uuid_t data_blocks[NUMBER_DIRECT_BLOCKS]; // ids of direct blocks
    uuid_t indirect_data_block;               // id of indirect data block
    time_t atime; // time of last access
    time_t mtime; // time of last modification
    time_t ctime; // time of last change to status

    nlink_t count; // reference count
} fcb;

typedef struct _dir_entry
{
    uuid_t fcb_id;                  // id of fcb
    char name[MAX_FILENAME_LENGTH]; // filename
} dir_entry;


#define MAX_DIRECTORY_ENTRIES_PER_BLOCK (BLOCK_SIZE / sizeof(dir_entry))
typedef struct _dir_data
{
    int cur_len; // used entries
    dir_entry entries[MAX_DIRECTORY_ENTRIES_PER_BLOCK];
} dir_data;

typedef struct _file_data
{
    int size;              // current size
    char data[BLOCK_SIZE]; // data
} file_data;

typedef struct
{
    fcb *fcb;
    int level;
    int index;
    int create; // for 1, it will create a new empty block
} fcb_iterator;

// We need to use a well-known value as a key for the root object.
const uuid_t ROOT_OBJECT_KEY = "root";

// This is the size of a regular key used to fetch things from the
// database. We use uuids as keys, so 16 bytes each
#define KEY_SIZE sizeof(uuid_t)

// The name of the file which will hold our filesystem
// If things get corrupted, unmount it and delete the file
// to start over with a fresh filesystem
#define DATABASE_NAME "myfs.db"

extern unqlite *pDb;

extern void error_handler(int);
void print_id(uuid_t *);

extern FILE *init_log_file();
extern void write_log(const char *, ...);

extern uuid_t zero_uuid;

// We can use the fs_state struct to pass information to fuse, which our handler functions can
// then access. In this case, we use it to pass a file handle for the file used for logging
struct myfs_state
{
    FILE *logfile;
};
#define NEWFS_PRIVATE_DATA ((struct myfs_state *)fuse_get_context()->private_data)

// Some helper functions for logging etc.

// In order to log actions while running through FUSE, we have to give
// it a file handle to use. We define a couple of helper functions to do
// logging. No need to change this if you don't see a need
//

FILE *logfile;

// Open a file for writing so we can obtain a handle
FILE *init_log_file()
{
    //Open logfile.
    logfile = fopen("myfs.log", "w");
    if (logfile == NULL)
    {
        perror("Unable to open log file. Life is not worth living.");
        exit(EXIT_FAILURE);
    }
    //Use line buffering
    setvbuf(logfile, NULL, _IOLBF, 0);
    return logfile;
}

// Write to the provided handle
void write_log(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    vfprintf(NEWFS_PRIVATE_DATA->logfile, format, ap);
}

// Simple error handler which cleans up and quits
void error_handler(int rc)
{
    if (rc != UNQLITE_OK)
    {
        const char *zBuf;
        int iLen;
        unqlite_config(pDb, UNQLITE_CONFIG_ERR_LOG, &zBuf, &iLen);
        if (iLen > 0)
        {
            perror("error_handler: ");
            perror(zBuf);
        }
        if (rc != UNQLITE_BUSY && rc != UNQLITE_NOTIMPLEMENTED)
        {
            /* Rollback */
            unqlite_rollback(pDb);
        }
        exit(rc);
    }
}

void print_id(uuid_t *id)
{
    size_t i;
    for (i = 0; i < sizeof *id; i++)
    {
        printf("%02x ", (*id)[i]);
    }
}
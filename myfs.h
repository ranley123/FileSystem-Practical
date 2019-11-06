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

#define MAX_FILENAME_SIZE 256
#define BLOCK_SIZE 4096
#define UUIDS_PER_BLOCK (BLOCK_SIZE / sizeof(uuid_t))

// Number of block references held in the FCB
#define NO_DIRECT_BLOCKS 15

#define MAX_FILE_SIZE (BLOCK_SIZE * NO_DIRECT_BLOCKS + BLOCK_SIZE * UUIDS_PER_BLOCK)

const mode_t DEFAULT_FILE_MODE = S_IFREG|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH;

const mode_t DEFAULT_DIR_MODE = S_IFDIR|S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IXGRP|S_IROTH|S_IXOTH;

typedef struct _DirectoryEntry {
    char name[MAX_FILENAME_SIZE]; // The name of the link
    uuid_t fcb_ref; // The uuid of the FCB the link points to
} DirectoryEntry;

#define DIRECTORY_ENTRIES_PER_BLOCK (4096 / sizeof(DirectoryEntry))

typedef struct _DirectoryDataBlock {
    int usedEntries;                                     // The number of used entries in the block. Used for fast traversal of empty or full blocks.
    DirectoryEntry entries[DIRECTORY_ENTRIES_PER_BLOCK]; // The entries stored
} DirectoryDataBlock;

typedef struct _FileDataBlock {
    int size;              // How much of the blocks it's being used
    char data[BLOCK_SIZE]; // The data block
} FileDataBlock;

typedef struct _FileControlBlock {
    mode_t          mode;                           // File type and mode
    uid_t           user_id;                        // User ID of owner 
    gid_t           group_id;                       // Group ID of owner 
    off_t           size;                           // Total size, in bytes 

    uuid_t          data_blocks[NO_DIRECT_BLOCKS];  // Direct references to data blocks
    
    uuid_t          indirectBlock;                  // Reference to a list of references to blocks

    struct timespec st_atim;                        // Time of last access
    struct timespec st_mtim;                        // Time of last modification
    struct timespec st_ctim;                        // Time of last status change
} FileControlBlock;

// We need to use a well-known value as a key for the root object.
const uuid_t ROOT_OBJECT_KEY = "LongLiveTheKing";

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

extern FILE* init_log_file();
extern void write_log(const char *, ...);

extern uuid_t zero_uuid;

// We can use the fs_state struct to pass information to fuse, which our handler functions can
// then access. In this case, we use it to pass a file handle for the file used for logging
struct myfs_state {
    FILE *logfile;
};
#define NEWFS_PRIVATE_DATA ((struct myfs_state *) fuse_get_context()->private_data)

// Some helper functions for logging etc.

// In order to log actions while running through FUSE, we have to give
// it a file handle to use. We define a couple of helper functions to do
// logging. No need to change this if you don't see a need
//

FILE *logfile;

// Open a file for writing so we can obtain a handle
FILE *init_log_file(){
    //Open logfile.
    logfile = fopen("myfs.log", "w");
    if (logfile == NULL) {
		perror("Unable to open log file. Life is not worth living.");
		exit(EXIT_FAILURE);
    }
    //Use line buffering
    setvbuf(logfile, NULL, _IOLBF, 0);
    return logfile;
}

// Write to the provided handle
void write_log(const char *format, ...){
    va_list ap;
    va_start(ap, format);
    vfprintf(NEWFS_PRIVATE_DATA->logfile, format, ap);
}

// Simple error handler which cleans up and quits
void error_handler(int rc){
	if( rc != UNQLITE_OK ){
		const char *zBuf;
		int iLen;
		unqlite_config(pDb,UNQLITE_CONFIG_ERR_LOG,&zBuf,&iLen);
		if( iLen > 0 ){
			perror("error_handler: ");
			perror(zBuf);
		}
		if( rc != UNQLITE_BUSY && rc != UNQLITE_NOTIMPLEMENTED ){
			/* Rollback */
			unqlite_rollback(pDb);
		}
		exit(rc);
	}
}

void print_id(uuid_t *id){
 	size_t i; 
    for (i = 0; i < sizeof *id; i ++) {
        printf("%02x ", (*id)[i]);
    }
}
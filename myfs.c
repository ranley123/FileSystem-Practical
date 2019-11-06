
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <libgen.h>

#include "myfs.h"

typedef struct
{
    char *parent;       // String containing the parent directory of a path
    char *name;         // String containing the name of the file a path points at
    char *savedPtrs[2]; // Pointers to be freed when the SplitPath is not needed anymore
} SplitPath;

unqlite *pDb; // Pointer used to reference the database connection

uuid_t zero_uuid; // An uuid that has all its bytes set to 0

dir_data emptyDirectory;
file_data emptyFile;

/**
 * Create a block iterator for the given FileControlBlock
 * @param [in] fcb      - The file control block whose data blocks to iterate
 * @param [in] create  - Set to not 0 if you want the iterator to create new blocks when a next block is requested
 * @return - The created iterator
 */
fcb_iterator make_iterator(fcb *fcb, int create)
{
    fcb_iterator iterator = {
        .fcb = fcb,
        .level = 0,
        .index = 0,
        .create = create};

    return iterator;
}

/**
 * Get the next block from a given FCB block iterator
 * @param [in]  iterator    - The iterator to retrieve the next block from
 * @param [out] block       - The data block to fill in with the retrieved block
 * @param [in]  blockSize   - The size of the block to be retrieved
 * @param [out] blockUUID   - If not NULL, gets filled in with the uuid of the retrieved block
 * @return The pointer passed in from block, or NULL if there are no more blocks to retrieve
 */
void *get_next_data_block(fcb_iterator *iterator, void *block, size_t blockSize, uuid_t *blockUUID)
{
    uuid_t next_block_id = {0};

    if (iterator->level > MAX_INDEX_LEVEL)
        return NULL;

    switch (iterator->level)
    {
    case 0:
    {
        if (iterator->create && uuid_compare(iterator->fcb->data_blocks[iterator->index], zero_uuid) == 0)
        {
            uuid_generate(next_block_id);
            char *empty_block = calloc(sizeof(char), blockSize);
            int rc = unqlite_kv_store(pDb, next_block_id, KEY_SIZE, empty_block, (ssize_t)blockSize);
            free(empty_block);
            error_handler(rc);
            uuid_copy(iterator->fcb->data_blocks[iterator->index], next_block_id);
        }
        else
        {
            uuid_copy(next_block_id, iterator->fcb->data_blocks[iterator->index]);
        }

        iterator->index += 1;
        if (iterator->index >= NUMBER_DIRECT_BLOCKS)
        {
            iterator->index = 0;
            iterator->level += 1;
        }
    }
    break;
    case 1:
    {
        if (iterator->index >= MAX_UUIDS_PER_BLOCK)
            return NULL;
        uuid_t blocks[MAX_UUIDS_PER_BLOCK] = {{0}};
        if (uuid_compare(iterator->fcb->indirectBlock, zero_uuid) == 0)
        {
            if (iterator->create)
            {
                uuid_generate(iterator->fcb->indirectBlock);
                int rc = unqlite_kv_store(pDb, iterator->fcb->indirectBlock, KEY_SIZE, &blocks, sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK);
                error_handler(rc);
            }
            else
            {
                return NULL;
            }
        }
        unqlite_int64 nBytes = MAX_UUIDS_PER_BLOCK * sizeof(uuid_t);
        int rc = unqlite_kv_fetch(pDb, iterator->fcb->indirectBlock, KEY_SIZE, *blocks, &nBytes);
        error_handler(rc);

        if (iterator->create && uuid_compare(blocks[iterator->index], zero_uuid) == 0)
        {
            uuid_generate(next_block_id);
            char *empty_block = calloc(sizeof(char), blockSize);
            rc = unqlite_kv_store(pDb, next_block_id, KEY_SIZE, empty_block, (ssize_t)blockSize);
            free(empty_block);
            error_handler(rc);
            uuid_copy(blocks[iterator->index], next_block_id);

            rc = unqlite_kv_store(pDb, iterator->fcb->indirectBlock, KEY_SIZE, *blocks, MAX_UUIDS_PER_BLOCK * sizeof(uuid_t));
            error_handler(rc);
        }
        else
        {
            uuid_copy(next_block_id, blocks[iterator->index]);
        }

        iterator->index += 1;
    }
    break;
    default:
        return NULL;
    }

    if (uuid_compare(next_block_id, zero_uuid) == 0)
        return NULL;

    if (blockUUID != NULL)
    {
        uuid_copy(*blockUUID, next_block_id);
    }

    unqlite_int64 nBytes = (unqlite_int64)blockSize;
    int rc = unqlite_kv_fetch(pDb, next_block_id, KEY_SIZE, block, &nBytes);
    error_handler(rc);

    return block;
}


/**
 * Split a path into it's filename and parent directory
 * @param [in]  path - The path to split
 * @return A SplitPath object containing the 2 parts of the path
 */
static SplitPath splitPath(const char *path)
{
    char *basenameCopy = strdup(path);
    char *dirnameCopy = strdup(path);

    char *bn = basename(basenameCopy);
    char *dn = dirname(dirnameCopy);

    SplitPath result = {
        .parent = dn,
        .name = bn,
        .savedPtrs = {dirnameCopy, basenameCopy}};

    return result;
}

/**
 * Free the pointers of a split path.
 * @param [in]  path - The SplitPath object to be freed
 */
static void freeSplitPath(SplitPath *path)
{
    free(path->savedPtrs[0]);
    free(path->savedPtrs[1]);
}

/**
 * Set a given timespec to current date and time
 * @param [out] tm - The timespec to set
 */
static void setTimespecToNow(struct timespec *tm)
{
    struct timespec now;
    timespec_get(&now, TIME_UTC);

    memcpy(tm, &now, sizeof(now));
}

/**
 * Fetch the root file control block from the database
 * @param [out] rootBlock - The block to be filled in with the root FCB
 * @return 0 if successful, <0 if an error happened.
 */
static int fetchRootFCB(fcb *rootBlock)
{

    unqlite_int64 nBytes = sizeof(fcb);
    int rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, KEY_SIZE, rootBlock, &nBytes);
    error_handler(rc);
    return 0;
}

/**
 * Initialize a FCB to be a new empty directory
 * @param [out] blockToFill - The block to initialize
 * @param [in]  mode        - The mode of the newly created node
 */
static void createDirectoryNode(fcb *blockToFill, mode_t mode)
{
    memset(blockToFill, 0, sizeof(fcb));
    blockToFill->mode = S_IFDIR | mode;

    setTimespecToNow(&blockToFill->ctime);
    setTimespecToNow(&blockToFill->atime);
    setTimespecToNow(&blockToFill->mtime);

    blockToFill->uid = getuid();
    blockToFill->gid = getgid();
}

/**
 * Initialize a FCB to be a new empty file
 * @param [out] blockToFill - The block to initialize
 * @param [in]  mode        - The mode of the newly created node
 */
static void createFileNode(fcb *blockToFill, mode_t mode)
{
    memset(blockToFill, 0, sizeof(fcb));
    blockToFill->mode = S_IFREG | mode;

    setTimespecToNow(&blockToFill->ctime);
    setTimespecToNow(&blockToFill->atime);
    setTimespecToNow(&blockToFill->mtime);

    blockToFill->uid = getuid();
    blockToFill->gid = getgid();
}

/**
 * Add a new link into a directory
 * @param [in,out]  dirBlock - The directory node in which to add the new link
 * @param [in]      name     - The name of the link
 * @param [in]      fcb_ref  - The uuid reference to the node to link to
 * @return 0 if successful, < 0 if an error happened.
 */
static int addFCBToDirectory(fcb *dirBlock, const char *name, const uuid_t fcb_ref)
{
    if (strlen(name) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    if (S_ISDIR(dirBlock->mode))
    {
        fcb_iterator iter = make_iterator(dirBlock, 1);
        dir_data entries;

        uuid_t blockUUID;

        while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
        {
            if (entries.used_entries == DIRECTORY_ENTRIES_PER_BLOCK)
            {
                continue;
            }

            for (int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i)
            {
                if (strcmp(entries.entries[i].name, name) == 0)
                {
                    return -EEXIST;
                }
            }

            for (int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i)
            {
                if (strcmp(entries.entries[i].name, "") == 0)
                {
                    strcpy(entries.entries[i].name, name);
                    uuid_copy(entries.entries[i].fcb_id, fcb_ref);

                    entries.used_entries += 1;
                    int rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                    error_handler(rc);
                    return 0;
                }
            }
        }

        return -ENOSPC;
    }

    return -ENOTDIR;
}

/**
 * Retrieve a node from a directory
 * @param [in]  dirBlock    - The directory node to retrieve the node from
 * @param [in]  name        - The name of the link to retrieve
 * @param [out] toFill      - The block to fill in with the retrieved FCB
 * @param [out] uuidToFill  - If not NULL, this is filled in with the reference to the retrieved node
 * @return 0 if successful, < 0 if an error happened.
 */
static int getFCBInDirectory(const fcb *dirBlock, const char *name, fcb *toFill, uuid_t *uuidToFill)
{
    if (strlen(name) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    if (S_ISDIR(dirBlock->mode))
    {
        fcb_iterator iter = make_iterator(dirBlock, 0);
        dir_data entries;

        uuid_t blockUUID;

        while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
        {

            for (int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i)
            {
                if (strcmp(entries.entries[i].name, name) == 0)
                {
                    unqlite_int64 bytesRead = sizeof(fcb);
                    int rc = unqlite_kv_fetch(pDb, entries.entries[i].fcb_id, KEY_SIZE, toFill, &bytesRead);

                    error_handler(rc);

                    if (uuidToFill != NULL)
                    {
                        uuid_copy(*uuidToFill, entries.entries[i].fcb_id);
                    }

                    if (bytesRead != sizeof(fcb))
                    {
                        write_log("FCB is corrupted. Exiting...\n");
                        exit(-1);
                    }

                    return 0;
                }
            }
        }
        return -ENOENT;
    }
    return -ENOTDIR;
}

/**
 * Get the file control block of the node found at the given path
 * @param [in]  path        - The path where the node should be located at
 * @param [out] toFill      - The block to be filled with the retrieved FCB.
 * @param [out] uuidToFill  - If not NULL, this is filled in with the uuid of the retrieved block
 * @return 0 if successful, < 0 if an error happened.
 */
static int getFCBAtPath(const char *path, fcb *toFill, uuid_t *uuidToFill)
{

    char *pathCopy = strdup(path);

    char *savePtr;

    char *p = strtok_r(pathCopy, "/", &savePtr);

    if (uuidToFill != NULL)
    {
        uuid_copy(*uuidToFill, ROOT_OBJECT_KEY);
    }
    fcb currentDir;
    fetchRootFCB(&currentDir);

    while (p != NULL)
    {

        int rc = getFCBInDirectory(&currentDir, p, &currentDir, uuidToFill);
        if (rc != 0)
            return rc;

        p = strtok_r(NULL, "/", &savePtr);
    }

    free(pathCopy);

    memcpy(toFill, &currentDir, sizeof currentDir);

    return 0;
}

/**
 * Retrieve the number of children of a given directory
 * @param [in] directory
 * @return The number of children the directory has, or a negative number on error
 */
static ssize_t numberOfChildren(const fcb *directory)
{
    if (S_ISDIR(directory->mode))
    {
        ssize_t noDirectories = 0;
        fcb_iterator iter = make_iterator(directory, 0);
        dir_data entries;

        uuid_t blockUUID;

        while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
        {
            noDirectories += entries.used_entries;
        }

        return noDirectories;
    }

    return -ENOTDIR;
}

/**
 * Remove all the data an FCB holds reference to
 * @param [in] fcb - The FCB whose data to remove
 */
static void removeNodeData(fcb *fcb)
{
    char fakeBlock[BLOCK_SIZE];
    uuid_t blockUUID;

    fcb_iterator iter = make_iterator(fcb, 0);

    while (get_next_data_block(&iter, fakeBlock, BLOCK_SIZE, &blockUUID))
    {
        int rc = unqlite_kv_delete(pDb, blockUUID, KEY_SIZE);
        error_handler(rc);
    }
}

/**
 * Remove the link from a directory, without deleting the node it points to
 * @param [in,out]  dirBlock - The block from which to remove the link
 * @param [in]      name     - The name of the link to remove
 * @return 0 if successful, < 0 if an error happened.
 */
static int unlinkLinkInDirectory(const fcb *dirBlock, const char *name)
{
    if (strlen(name) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    if (dirBlock->mode & S_IFDIR)
    {
        fcb_iterator iter = make_iterator(dirBlock, 0);
        dir_data entries;

        uuid_t blockUUID;

        while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
        {
            for (int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i)
            {
                if (strcmp(entries.entries[i].name, name) == 0)
                {
                    memset(&entries.entries[i], 0, sizeof(entries.entries[i]));
                    entries.used_entries -= 1;
                    int rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                    error_handler(rc);

                    return 0;
                }
            }
        }
        return -ENOENT;
    }
    return -ENOTDIR;
}

/**
 * Remove a file from a directory. This both unlinks the file and deletes the node the link pointed to.
 * @param [in,out]  dirBlock - The directory node that should have the file removed
 * @param [in]      name     - The name of the file to be removed
 * @return 0 if successful, < 0 if an error happened.
 */
static int removeFileFCBinDirectory(const fcb *dirBlock, const char *name)
{
    if (strlen(name) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    if (dirBlock->mode & S_IFDIR)
    {
        fcb_iterator iter = make_iterator(dirBlock, 0);
        dir_data entries;

        uuid_t blockUUID;

        while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
        {

            for (int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i)
            {
                if (strcmp(entries.entries[i].name, name) == 0)
                {
                    uuid_t fcb_uuid;
                    fcb fcb;
                    memcpy(&fcb_uuid, entries.entries[i].fcb_id, sizeof(uuid_t));

                    unqlite_int64 bytesRead = sizeof(fcb);

                    int rc = unqlite_kv_fetch(pDb, fcb_uuid, KEY_SIZE, &fcb, &bytesRead);
                    error_handler(rc);

                    if (S_ISREG(fcb.mode))
                    {

                        removeNodeData(&fcb);

                        unqlite_kv_delete(pDb, fcb_uuid, KEY_SIZE);

                        memset(&entries.entries[i], 0, sizeof(entries.entries[i]));

                        entries.used_entries -= 1;
                        rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                        error_handler(rc);
                    }
                    else
                    {
                        return -EISDIR;
                    }

                    return 0;
                }
            }
        }
        return -ENOENT;
    }
    return -ENOTDIR;
}

/**
 * Remove a directory from a directory. This both unlinks the directory and deletes the node the link pointed to.
 * @param [in,out]  dirBlock - The directory node that should have the directory removed
 * @param [in]      name     - The name of the directory to be removed
 * @return 0 if successful, < 0 if an error happened.
 */
static int removeDirectoryFCBinDirectory(const fcb *dirBlock, const char *name)
{
    if (strlen(name) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    if (dirBlock->mode & S_IFDIR)
    {
        fcb_iterator iter = make_iterator(dirBlock, 0);
        dir_data entries;

        uuid_t blockUUID;

        while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
        {
            for (int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i)
            {
                if (strcmp(entries.entries[i].name, name) == 0)
                {
                    uuid_t fcb_uuid;
                    fcb fcb;
                    memcpy(&fcb_uuid, entries.entries[i].fcb_id, sizeof(uuid_t));

                    unqlite_int64 bytesRead = sizeof(fcb);

                    int rc = unqlite_kv_fetch(pDb, fcb_uuid, KEY_SIZE, &fcb, &bytesRead);
                    error_handler(rc);

                    if (S_ISDIR(fcb.mode))
                    {

                        ssize_t noChildren = numberOfChildren(&fcb);

                        if (noChildren != 0)
                        {
                            return -ENOTEMPTY;
                        }

                        removeNodeData(&fcb);
                        unqlite_kv_delete(pDb, fcb_uuid, KEY_SIZE);

                        memset(&entries.entries[i], 0, sizeof(entries.entries[i]));

                        entries.used_entries -= 1;
                        rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &entries, sizeof entries);
                        error_handler(rc);

                        return 0;
                    }
                    else
                    {
                        return -ENOTDIR;
                    }
                }
            }
        }

        return -ENOENT;
    }
    return -ENOTDIR;
}

/**
 * Initialize the file system before mounting
 */
static void init_fs()
{
    int rc = unqlite_open(&pDb, DATABASE_NAME, UNQLITE_OPEN_CREATE);
    if (rc != UNQLITE_OK)
        error_handler(rc);

    unqlite_int64 bytesRead;

    rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, KEY_SIZE, NULL, &bytesRead);

    if (rc == UNQLITE_NOTFOUND)
    {
        perror("Root of filesystem not found. Creating it...\n");
        fcb rootDirectory;

        createDirectoryNode(&rootDirectory, DEFAULT_DIR_MODE);

        rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, KEY_SIZE, &rootDirectory, sizeof rootDirectory);

        error_handler(rc);
    }
    else
    {
        perror("Root of filesystem found. Using it as the root folder...\n");
        error_handler(rc);

        if (bytesRead != sizeof(fcb))
        {
            perror("!!! Database is corrupted, exiting...\n");
            exit(-1);
        }
    }
}

// The functions which follow are handler functions for various things a filesystem needs to do:
// reading, getting attributes, truncating, etc. They will be called by FUSE whenever it needs
// your filesystem to do something, so this is where functionality goes.

/**
 * Get file and directory attributes (meta-data).
 * @param [in]  path    - The path of the file whose metadata to retrieve
 * @param [in]  stbuf   - The stat buffer to fill with the metadata
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_getattr(const char *path, struct stat *stbuf)
{
    write_log("myfs_getattr(path=\"%s\")\n", path);

    fcb currentDirectory;

    int rc = getFCBAtPath(path, &currentDirectory, NULL);

    if (rc != 0)
        return rc;

    stbuf->st_mode = currentDirectory.mode;            /* File mode.  */
    stbuf->st_nlink = 2;                               /* Link count.  */
    stbuf->st_uid = currentDirectory.uid;          /* User ID of the file's owner.  */
    stbuf->st_gid = currentDirectory.gid;         /* Group ID of the file's group. */
    stbuf->st_size = currentDirectory.size;            /* Size of file, in bytes.  */
    stbuf->st_atime = currentDirectory.atime.tv_sec; /* Time of last access.  */
    stbuf->st_mtime = currentDirectory.mtime.tv_sec; /* Time of last modification.  */
    stbuf->st_ctime = currentDirectory.ctime.tv_sec; /* Time of last status change.  */

    return 0;
}

// Read a directory.
// Read 'man 2 readdir'.
/**
 * List the links in a directory at a given path
 * @param [in]  path    - The path to the directory whose links to iterate
 * @param [out] buf     - An opaque buffer that needs to be passed to the filler function
 * @param [in]  filler  - A function used to fill the buffer with the retrieved links
 * @param [in]  offset  - !!UNUSED!!
 * @param [in]  fi      - File handler (currently not used)
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    write_log("myfs_readdir(path=\"%s\")\n", path);

    fcb currentDirectory;

    int rc = getFCBAtPath(path, &currentDirectory, NULL);
    if (rc != 0)
        return rc;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    fcb_iterator iter = make_iterator(&currentDirectory, 0);
    dir_data entries;

    uuid_t blockUUID;

    while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
    {
        for (int i = 0; i < DIRECTORY_ENTRIES_PER_BLOCK; ++i)
        {
            if (strcmp(entries.entries[i].name, "") != 0)
            {
                filler(buf, entries.entries[i].name, NULL, 0);
            }
        }
    }

    return 0;
}

/**
 * Reads data from a filesystem block
 * @param [out] dest    - The buffer to fill in with the read information
 * @param [in]  block   - The block to retrieve the information from
 * @param [in]  offset  - The offset from where to read data from
 * @param [in]  size    - The number of bytes to be read
 * @return 0 if successful, < 0 if an error happened.
 */
static int readFromBlock(char *dest, file_data *block, off_t offset, size_t size)
{
    off_t start = offset, end = start + size;

    if (start >= block->size)
        return -1;

    unsigned bytesToRead = (unsigned)min(block->size - start, end - start);

    memcpy(dest, &block->data[offset], bytesToRead);
    return bytesToRead;
}

/**
 * Read data from a file.
 * @param [in]  path    - The path of the file to read from
 * @param [out] buf     - The buffer to fill in with the read information
 * @param [in]  size    - The number of bytes to be read
 * @param [in]  offset  - The offset from where to read data from
 * @param [in]  fi      - File handler (currently not used)
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    write_log("myfs_read(path=\"%s\", size=%zu, offset=%zu", path, size, offset);

    fcb fcb;

    int rc = getFCBAtPath(path, &fcb, NULL);

    if (rc != 0)
        return rc;

    unsigned bytesRead = 0;
    if (S_ISREG(fcb.mode))
    {

        fcb_iterator iter = make_iterator(&fcb, 0);
        file_data dataBlock;

        while (get_next_data_block(&iter, &dataBlock, sizeof(dataBlock), NULL))
        {
            if (size == 0)
                break;
            off_t bR = readFromBlock(&buf[bytesRead], &dataBlock, offset, size);

            if (bR < 0)
            {
                offset -= BLOCK_SIZE;
            }
            else
            {
                bytesRead += bR;
                offset = max(0, offset - bR);
                size -= bR;
            }
        }

        return bytesRead;
    }
    else
    {
        return -EISDIR;
    }
}

/**
 * Create a new file
 * @param [in] path - The path at which to create the new file
 * @param [in] mode - The permissions of the new file
 * @param [in] fi   - File handler (currently not used)
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    write_log("myfs_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n", path, mode, fi);

    SplitPath newPath = splitPath(path);

    if (strlen(newPath.name) >= MAX_FILENAME_LENGTH)
    {
        freeSplitPath(&newPath);
        return -ENAMETOOLONG;
    }

    fcb currentDir;

    uuid_t parentFCBUUID;
    int rc = getFCBAtPath(newPath.parent, &currentDir, &parentFCBUUID);

    if (rc != 0)
    {
        freeSplitPath(&newPath);
        return rc;
    }

    fcb newFCB;

    createFileNode(&newFCB, mode);

    uuid_t newFileRef = {0};

    uuid_generate(newFileRef);

    rc = unqlite_kv_store(pDb, newFileRef, KEY_SIZE, &newFCB, sizeof newFCB);

    error_handler(rc);

    rc = addFCBToDirectory(&currentDir, newPath.name, newFileRef);

    // In case new blocks were added.
    int dbRc = unqlite_kv_store(pDb, parentFCBUUID, KEY_SIZE, &currentDir, sizeof(currentDir));
    error_handler(dbRc);

    // TODO: Add error handling for when the name is already used. Currently, the DB is populated with something that has no
    // reference.

    freeSplitPath(&newPath);
    return rc;
}

/**
 * Update the times (actime, modtime) for a file.
 * @param [in] path - The path of the node whose timestamps to update
 * @param [in] tv   - The values to which the timestamps should be set to.
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_utimens(const char *path, const struct timespec tv[2])
{
    write_log("myfs_utimens(path=\"%s\")\n", path);

    fcb fcb;
    uuid_t fcbUUID;

    int rc = getFCBAtPath(path, &fcb, &fcbUUID);

    if (rc != 0)
        return rc;

    memcpy(&fcb.atime, &tv[0], sizeof(struct timespec));
    memcpy(&fcb.mtime, &tv[1], sizeof(struct timespec));

    rc = unqlite_kv_store(pDb, fcbUUID, KEY_SIZE, &fcb, sizeof(fcb));

    error_handler(rc);

    return 0;
}

/**
 * Write data to a given data block
 * @param [out] dest    - The block to write data into
 * @param [in]  buf     - The data to write into the block
 * @param [in]  offset  - The point at which to start writing data into the block
 * @param [in]  size    - The amount of data to write into the block
 * @return 0 if successful, < 0 if an error happened.
 */
static int writeToBlock(file_data *dest, const char *buf, off_t offset, size_t size)
{
    off_t start = offset, end = start + size;

    if (start >= BLOCK_SIZE)
        return -1;

    unsigned bytesWritten = (unsigned)(min(end, BLOCK_SIZE) - start);

    memcpy(&dest->data[start], buf, bytesWritten);

    dest->size = (int)(start + bytesWritten);

    return bytesWritten;
}

/**
 * Write data to a file
 * @param [in] path     - Path to the file in which to write the data
 * @param [in]  buf     - The data to write into the file
 * @param [in]  offset  - The point at which to start writing data into the file
 * @param [in]  size    - The amount of data to write into the file
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    write_log("myfs_write(path=\"%s\", size=%d, offset=%lld,)\n", path, size, offset);

    fcb fcb;
    uuid_t fileUUID;
    int rc = getFCBAtPath(path, &fcb, &fileUUID);

    if (rc != 0)
        return rc;
    if (size == 0)
        return 0;

    off_t savedOffset = offset;

    int bytesWritten = 0;
    if (S_ISREG(fcb.mode))
    {
        uuid_t blockUUID;
        file_data dataBlock;
        fcb_iterator iter = make_iterator(&fcb, 1);

        while (get_next_data_block(&iter, &dataBlock, sizeof(dataBlock), &blockUUID))
        {
            int bW = writeToBlock(&dataBlock, &buf[bytesWritten], offset, size);
            if (bW == -1)
            {
                offset -= BLOCK_SIZE;
            }
            else
            {
                rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &dataBlock, sizeof(dataBlock));
                error_handler(rc);
                offset = max(0, offset - bW);
                size -= bW;
                bytesWritten += bW;
            }
            if (size == 0)
                break;
        }

        fcb.size = max(fcb.size, savedOffset + bytesWritten);

        rc = unqlite_kv_store(pDb, fileUUID, KEY_SIZE, &fcb, sizeof(fcb));
        error_handler(rc);

        return bytesWritten;
    }
    else
    {
        return -EISDIR;
    }
}

/**
 * Set the size of a file. This allocates new blocks or removes blocks so that the file is the correct size
 * @param [in] path     - The path of the file to resize
 * @param [in] newSize  - The new size of the file
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_truncate(const char *path, off_t newSize)
{
    write_log("myfs_truncate(path=\"%s\", newSize=%lld)\n", path, newSize);

    off_t remainingSize = newSize;

    if (newSize > MAX_FILE_SIZE)
    {
        return -EFBIG;
    }

    uuid_t fileUUID;
    fcb fileToResize;

    int rc = getFCBAtPath(path, &fileToResize, &fileUUID);

    if (rc != 0)
        return rc;

    for (int blockIdx = 0; blockIdx < NUMBER_DIRECT_BLOCKS; ++blockIdx)
    {
        if (remainingSize == 0)
        {
            if (uuid_compare(fileToResize.data_blocks[blockIdx], zero_uuid) != 0)
            {
                unqlite_kv_delete(pDb, fileToResize.data_blocks[blockIdx], KEY_SIZE);
                memset(&fileToResize.data_blocks[blockIdx], 0, KEY_SIZE);
            }
        }
        else
        {
            if (uuid_compare(fileToResize.data_blocks[blockIdx], zero_uuid) == 0)
            {
                uuid_t newBlockUUID = {0};
                uuid_generate(newBlockUUID);

                file_data block = emptyFile;
                block.size = (int)min(remainingSize, BLOCK_SIZE);
                remainingSize -= block.size;

                rc = unqlite_kv_store(pDb, newBlockUUID, KEY_SIZE, &block, sizeof(block));
                error_handler(rc);

                uuid_copy(fileToResize.data_blocks[blockIdx], newBlockUUID);
            }
        }
    }

    // Handle first level of indirection
    if (remainingSize == 0)
    {
        if (uuid_compare(fileToResize.indirectBlock, zero_uuid) != 0)
        {
            uuid_t blocks[MAX_UUIDS_PER_BLOCK];
            unqlite_int64 nBytes = sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK;
            rc = unqlite_kv_fetch(pDb, fileToResize.indirectBlock, KEY_SIZE, &blocks, &nBytes);
            error_handler(rc);

            for (int blockIdx = 0; blockIdx < MAX_UUIDS_PER_BLOCK; ++blockIdx)
            {
                if (uuid_compare(blocks[blockIdx], zero_uuid) != 0)
                {
                    unqlite_kv_delete(pDb, blocks[blockIdx], KEY_SIZE);
                    memset(&blocks[blockIdx], 0, KEY_SIZE);
                }
            }
        }
        unqlite_kv_delete(pDb, fileToResize.indirectBlock, KEY_SIZE);
        memset(fileToResize.indirectBlock, 0, KEY_SIZE);
    }
    else
    {
        uuid_t blocks[MAX_UUIDS_PER_BLOCK] = {{0}};
        if (uuid_compare(fileToResize.indirectBlock, zero_uuid) == 0)
        {
            uuid_t indirectBlockUUID = {0};
            uuid_generate(indirectBlockUUID);

            rc = unqlite_kv_store(pDb, indirectBlockUUID, KEY_SIZE, &blocks, sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK);
            error_handler(rc);

            uuid_copy(fileToResize.indirectBlock, indirectBlockUUID);
        }

        unqlite_int64 nBytes = sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK;
        rc = unqlite_kv_fetch(pDb, fileToResize.indirectBlock, KEY_SIZE, &blocks, &nBytes);
        error_handler(rc);

        for (int blockIdx = 0; blockIdx < MAX_UUIDS_PER_BLOCK; ++blockIdx)
        {
            if (remainingSize == 0)
            {
                if (uuid_compare(blocks[blockIdx], zero_uuid) != 0)
                {
                    unqlite_kv_delete(pDb, blocks[blockIdx], KEY_SIZE);
                    memset(&blocks[blockIdx], 0, KEY_SIZE);
                }
            }
            else
            {
                if (uuid_compare(blocks[blockIdx], zero_uuid) == 0)
                {
                    uuid_t newBlockUUID = {0};
                    uuid_generate(newBlockUUID);

                    file_data block = emptyFile;
                    block.size = (int)min(remainingSize, BLOCK_SIZE);
                    remainingSize -= block.size;

                    rc = unqlite_kv_store(pDb, newBlockUUID, KEY_SIZE, &block, sizeof(block));
                    error_handler(rc);

                    uuid_copy(blocks[blockIdx], newBlockUUID);
                }
            }
        }
        rc = unqlite_kv_store(pDb, fileToResize.indirectBlock, KEY_SIZE, &blocks, sizeof(blocks));
        error_handler(rc);
    }

    fileToResize.size = newSize;

    unqlite_int64 nBytes = sizeof(fileToResize);
    rc = unqlite_kv_store(pDb, fileUUID, KEY_SIZE, &fileToResize, nBytes);

    error_handler(rc);

    return 0;
}

// Set permissions.
// Read 'man 2 chmod'.
/**
 * Set the permissions of a file at a given path
 * @param [in] path - The path of the file whose permissions to change
 * @param [in] mode - The new permisison
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_chmod(const char *path, mode_t mode)
{
    write_log("myfs_chmod(path=\"%s\", mode=0%03o)\n", path, mode);

    fcb block;
    uuid_t blockUUID;
    int rc = getFCBAtPath(path, &block, &blockUUID);

    if (rc != 0)
        return rc;

    block.mode |= mode;

    rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &block, sizeof(block));

    error_handler(rc);

    return 0;
}

/**
 * Change owning user and group of a given file
 * @param [in] path - The path of the file whose owners to change
 * @param [in] uid  - The new owning user
 * @param [in] gid  - The new owning group
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_chown(const char *path, uid_t uid, gid_t gid)
{
    write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);

    fcb block;
    uuid_t blockUUID;

    int rc = getFCBAtPath(path, &block, &blockUUID);

    if (rc != 0)
        return rc;

    block.uid = uid;
    block.gid = gid;

    rc = unqlite_kv_store(pDb, blockUUID, KEY_SIZE, &block, sizeof(block));

    error_handler(rc);

    return 0;
}

/**
 * Create a new directory at the given path
 * @param [in] path - The path at which to create the new directory
 * @param [in] mode - The permissions of the new directory
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_mkdir(const char *path, mode_t mode)
{
    write_log("myfs_mkdir(path=\"%s\", mode=0%03o)\n", path, mode);

    SplitPath newPath = splitPath(path);

    if (strlen(newPath.name) >= MAX_FILENAME_LENGTH)
    {
        freeSplitPath(&newPath);
        return -ENAMETOOLONG;
    }

    fcb currentDir;
    uuid_t parentFCBUUID;
    int rc = getFCBAtPath(newPath.parent, &currentDir, &parentFCBUUID);

    if (rc != 0)
    {
        freeSplitPath(&newPath);
        return rc;
    }

    fcb newDirectory;
    createDirectoryNode(&newDirectory, mode);

    uuid_t newDirectoryRef = {0};

    uuid_generate(newDirectoryRef);

    rc = unqlite_kv_store(pDb, newDirectoryRef, KEY_SIZE, &newDirectory, sizeof newDirectory);

    error_handler(rc);

    rc = addFCBToDirectory(&currentDir, newPath.name, newDirectoryRef);

    // In case new blocks were added.
    int dbRc = unqlite_kv_store(pDb, parentFCBUUID, KEY_SIZE, &currentDir, sizeof(currentDir));
    error_handler(dbRc);

    // TODO: Add error handling for when the name is already used. Currently, the DB is populated with something that has no
    // reference.

    freeSplitPath(&newPath);
    return rc;
}

/**
 * Remove the file at the given path
 * @param [in] path - The path of the file to be removed
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_unlink(const char *path)
{
    write_log("myfs_unlink(path=\"%s\")\n", path);

    SplitPath pathToRemove = splitPath(path);

    fcb parentDir;

    int rc = getFCBAtPath(pathToRemove.parent, &parentDir, NULL);

    if (rc != 0)
    {
        freeSplitPath(&pathToRemove);
        return rc;
    }

    rc = removeFileFCBinDirectory(&parentDir, pathToRemove.name);

    freeSplitPath(&pathToRemove);

    return rc;
}

/**
 * Remove the directory at the given path
 * @param [in] path - The path of the directory to be removed
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_rmdir(const char *path)
{
    write_log("myfs_rmdir(path=\"%s\")\n", path);

    SplitPath pathToRemove = splitPath(path);

    fcb parentDir;

    int rc = getFCBAtPath(pathToRemove.parent, &parentDir, NULL);

    if (rc != 0)
    {
        freeSplitPath(&pathToRemove);
        return rc;
    }

    rc = removeDirectoryFCBinDirectory(&parentDir, pathToRemove.name);

    freeSplitPath(&pathToRemove);
    return rc;
}

static int myfs_rename(const char *from, const char *to)
{
    write_log("myfs_rename(from=\"%s\", to=\"%s\")", from, to);

    SplitPath fromPath = splitPath(from);

    uuid_t sourceDirUUID;
    fcb sourceDirFCB;

    int rc = getFCBAtPath(fromPath.parent, &sourceDirFCB, &sourceDirUUID);
    if (rc != 0)
    {
        freeSplitPath(&fromPath);
        return rc;
    }
    uuid_t sourceUUID;
    fcb sourceFCB;

    rc = getFCBInDirectory(&sourceDirFCB, fromPath.name, &sourceFCB, &sourceUUID);
    if (rc != 0)
    {
        freeSplitPath(&fromPath);
        return rc;
    }

    SplitPath toPath = splitPath(to);

    uuid_t targetDirUUID = {0};
    fcb targetParentFCB;
    fcb *targetParentFCBPtr;

    if (strcmp(toPath.parent, fromPath.parent) == 0)
    {
        targetParentFCBPtr = &sourceDirFCB;
        uuid_copy(targetDirUUID, sourceDirUUID);
    }
    else
    {
        targetParentFCBPtr = &targetParentFCB;
        rc = getFCBAtPath(toPath.parent, targetParentFCBPtr, &targetDirUUID);
        if (rc != 0)
        {
            freeSplitPath(&fromPath);
            freeSplitPath(&toPath);
            return rc;
        }
    }

    fcb targetFcb;

    rc = getFCBInDirectory(targetParentFCBPtr, toPath.name, &targetFcb, NULL);

    if (rc == 0)
    {
        if (S_ISDIR(targetFcb.mode))
        {
            rc = removeDirectoryFCBinDirectory(targetParentFCBPtr, toPath.name);
            if (rc != 0)
            {
                freeSplitPath(&fromPath);
                freeSplitPath(&toPath);
                return rc;
            }
        }
        else
        {
            rc = removeFileFCBinDirectory(targetParentFCBPtr, toPath.name);
            if (rc != 0)
            {
                freeSplitPath(&fromPath);
                freeSplitPath(&toPath);
                return rc;
            }
        }

        rc = addFCBToDirectory(targetParentFCBPtr, toPath.name, sourceUUID);
        if (rc != 0)
        {
            freeSplitPath(&fromPath);
            freeSplitPath(&toPath);
            return rc;
        }
    }
    else if (rc == -ENOENT)
    {
        rc = addFCBToDirectory(targetParentFCBPtr, toPath.name, sourceUUID);
        if (rc != 0)
        {
            freeSplitPath(&fromPath);
            freeSplitPath(&toPath);
            return rc;
        }
    }
    else
    {
        freeSplitPath(&fromPath);
        freeSplitPath(&toPath);
        return rc;
    }

    rc = unlinkLinkInDirectory(&sourceDirFCB, fromPath.name);
    if (rc != 0)
    {
        freeSplitPath(&fromPath);
        freeSplitPath(&toPath);
        return rc;
    }

    rc = unqlite_kv_store(pDb, targetDirUUID, KEY_SIZE, targetParentFCBPtr, sizeof(targetParentFCB));
    error_handler(rc);

    rc = unqlite_kv_store(pDb, sourceDirUUID, KEY_SIZE, &sourceDirFCB, sizeof(sourceDirFCB));
    error_handler(rc);

    freeSplitPath(&fromPath);
    freeSplitPath(&toPath);
    return 0;
}

// This struct contains pointers to all the functions defined above
// It is used to pass the function pointers to fuse
// fuse will then execute the methods as required
static struct fuse_operations myfs_oper = {
    .getattr = myfs_getattr,
    .readdir = myfs_readdir,
    .mkdir = myfs_mkdir,
    .rmdir = myfs_rmdir,
    .read = myfs_read,
    .create = myfs_create,
    .utimens = myfs_utimens,
    .write = myfs_write,
    .truncate = myfs_truncate,
    .unlink = myfs_unlink,
    .chown = myfs_chown,
    .chmod = myfs_chmod,
    .rename = myfs_rename};

void shutdown_fs()
{
    unqlite_close(pDb);
}

int main(int argc, char *argv[])
{
    int fuserc;
    struct myfs_state *myfs_internal_state;

    //Setup the log file and store the FILE* in the private data object for the file system.
    myfs_internal_state = malloc(sizeof(struct myfs_state));
    myfs_internal_state->logfile = init_log_file();

    uuid_clear(zero_uuid);

    //Initialise the file system. This is being done outside of fuse for ease of debugging.
    init_fs();

    // Now pass our function pointers over to FUSE, so they can be called whenever someone
    // tries to interact with our filesystem. The internal state contains a file handle
    // for the logging mechanism
    fuserc = fuse_main(argc, argv, &myfs_oper, myfs_internal_state);

    //Shutdown the file system.
    shutdown_fs();

    return fuserc;
}
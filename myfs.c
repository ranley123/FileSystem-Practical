#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <libgen.h>

#include "myfs.h"

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
 * @param id 	- Given the id, retrieve
 * @param data 	- where returning results will be put into
 * @param size 	- expected size of fetched results
 */
void read_from_db(uuid_t id, void *data, size_t size)
{
    int rc;
    unqlite_int64 nBytes = size;

    // check if correct results found
    rc = unqlite_kv_fetch(pDb, id, KEY_SIZE, NULL, &nBytes);
    error_handler(rc);

    // check if we retrieved the correct thing with correct size
    if (nBytes != size)
    {
        write_log("fetch: Unexpected size. Expected %d, current %d\n", size, nBytes);
        exit(-1);
    }

    // retrieve the fcb
    unqlite_kv_fetch(pDb, id, KEY_SIZE, data, &nBytes);
}

/**
 * Write fcb back to the database
 * @param id 	- Given the id, store
 * @param data 	- data to be put into the database
 * @param size 	- size of data
 */
void write_to_db(uuid_t id, void *data, size_t size)
{
    int rc = unqlite_kv_store(pDb, id, KEY_SIZE, data, size);
    error_handler(rc);
}

uuid_t *get_next_direct_block(fcb_iterator *iterator, size_t block_size, uuid_t *next_block_id)
{
    if (iterator->level != 0)
        return NULL;
    if (iterator->create && uuid_compare(iterator->fcb->data_blocks[iterator->index], zero_uuid) == 0)
    {
        uuid_generate(*next_block_id);
        char *empty_block = calloc(sizeof(char), block_size);
        write_to_db(*next_block_id, empty_block, block_size);
        free(empty_block);
        uuid_copy(iterator->fcb->data_blocks[iterator->index], *next_block_id);
    }
    else
        uuid_copy(*next_block_id, iterator->fcb->data_blocks[iterator->index]);

    iterator->index += 1;
    if (iterator->index >= NUMBER_DIRECT_BLOCKS)
    {
        iterator->index = 0;
        iterator->level += 1;
    }
    return next_block_id;
}

uuid_t *get_next_indirect_block(fcb_iterator *iterator, size_t block_size, uuid_t *next_block_id)
{
    if (iterator->level == 1 || iterator->index >= MAX_UUIDS_PER_BLOCK)
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
        char *empty_block = calloc(sizeof(char), block_size);
        rc = unqlite_kv_store(pDb, next_block_id, KEY_SIZE, empty_block, block_size);
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
    return next_block_id;
}

/**
 * Get the next block from a given FCB block iterator
 * @param [in]  iterator    - The iterator to retrieve the next block from
 * @param [out] block       - The data block to fill in with the retrieved block
 * @param [in]  blockSize   - The size of the block to be retrieved
 * @param [out] blockUUID   - If not NULL, gets filled in with the uuid of the retrieved block
 * @return The pointer passed in from block, or NULL if there are no more blocks to retrieve
 */
void *get_next_data_block(fcb_iterator *iterator, void *block, size_t block_size, uuid_t *block_id)
{
    uuid_t next_block_id = {0}; // a temporary variable to store

    if (iterator->level > MAX_INDEX_LEVEL)
        return NULL;

    switch (iterator->level)
    {
    case 0: // level is 0: we are iterating over direct blocks
    {
        if (get_next_direct_block(iterator, block_size, &next_block_id) == NULL)
            return NULL;
    }
    break;
    case 1: // level is 1: we are iterating over blocks in indirect block
    {
        if (get_next_indirect_block(iterator, block_size, &next_block_id))
            return NULL;
    }
    break;
    default:
        return NULL;
    }

    // if no valid next block id is retrieved
    if (uuid_compare(next_block_id, zero_uuid) == 0)
        return NULL;

    // when block_id is NULL, it means users do not want this information
    if (block_id != NULL)
        uuid_copy(*block_id, next_block_id);

    // retrieve the next block to return
    read_from_db(next_block_id, block, block_size);
    return block;
}

/**
 * Fetch the root file control block from the database
 * @param [out] rootBlock - The block to be filled in with the root FCB
 * @return 0 if successful, <0 if an error happened.
 */
static int get_root_fcb(fcb *root_fcb)
{
    read_from_db(ROOT_OBJECT_KEY, root_fcb, sizeof(fcb));
    return 0;
}

/**
 * Initialize a FCB to be a new empty directory
 * @param [out] blockToFill - The block to initialize
 * @param [in]  mode        - The mode of the newly created node
 */
static void make_new_fcb(fcb *cur_fcb, mode_t mode, int dir)
{
    memset(cur_fcb, 0, sizeof(fcb));
    if (dir)
        cur_fcb->mode = S_IFDIR | mode;
    else
        cur_fcb->mode = S_IFREG | mode;

    time_t now = time(NULL);
    cur_fcb->atime = now;
    cur_fcb->mtime = now;
    cur_fcb->ctime = now;

    cur_fcb->uid = getuid();
    cur_fcb->gid = getgid();
}

/**
 * Add a new link into a directory
 * @param [in,out]  dirBlock - The directory node in which to add the new link
 * @param [in]      name     - The name of the link
 * @param [in]      fcb_ref  - The uuid reference to the node to link to
 * @return 0 if successful, < 0 if an error happened.
 */
int add_fcb_to_dir(fcb *parent_dir_fcb, const char *name, const uuid_t fcb_id)
{
    if (strlen(name) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    if (!S_ISDIR(parent_dir_fcb->mode))
        return -ENOTDIR;

    fcb_iterator iterator = make_iterator(parent_dir_fcb, 1);
    dir_data parent_dir_data;
    uuid_t data_block_id;

    while (get_next_data_block(&iterator, &parent_dir_data, sizeof(parent_dir_data), &data_block_id) != NULL)
    {
        if (parent_dir_data.used_entries == MAX_DIRECTORY_ENTRIES_PER_BLOCK)
            continue;

        for (int i = 0; i < MAX_DIRECTORY_ENTRIES_PER_BLOCK; ++i)
        {
            if (strcmp(parent_dir_data.entries[i].name, "") == 0)
            {
                // set up the current empty entry
                uuid_copy(parent_dir_data.entries[i].fcb_id, fcb_id);
                strcpy(parent_dir_data.entries[i].name, name);
                // update the used count
                parent_dir_data.used_entries += 1;

                // update the parent dir data
                write_to_db(data_block_id, &parent_dir_data, sizeof(dir_data));
                return 0;
            }
        }
    }

    return -ENOSPC; // no extra space
}

/**
 * Retrieve a node from a directory
 * @param [in]  dirBlock    - The directory node to retrieve the node from
 * @param [in]  name        - The name of the link to retrieve
 * @param [out] toFill      - The block to fill in with the retrieved FCB
 * @param [out] uuidToFill  - If not NULL, this is filled in with the reference to the retrieved node
 * @return 0 if successful, < 0 if an error happened.
 */
static int get_fcb_from_dir(const fcb *dirBlock, const char *name, fcb *toFill, uuid_t *uuidToFill)
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

            for (int i = 0; i < MAX_DIRECTORY_ENTRIES_PER_BLOCK; ++i)
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
static int get_fcb_by_path(const char *path, fcb *toFill, uuid_t *uuidToFill, int get_parent)
{
    write_log("get fcb by path \n");
    char *path_copy = strdup(path);
    char *path_remaining;

    // if parent fcb wanted, remove the base name
    if (get_parent)
    {
        path_copy = dirname(path_copy);
    }

    char *cur_name = strtok_r(path_copy, "/", &path_remaining);

    if (uuidToFill != NULL)
    {
        uuid_copy(*uuidToFill, ROOT_OBJECT_KEY);
    }
    fcb parent_dir;
    get_root_fcb(&parent_dir);

    while (cur_name != NULL)
    {

        int rc = get_fcb_from_dir(&parent_dir, cur_name, &parent_dir, uuidToFill);
        if (rc != 0)
            return rc;

        cur_name = strtok_r(NULL, "/", &path_remaining);
    }

    free(path_copy);

    memcpy(toFill, &parent_dir, sizeof(fcb));

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
            for (int i = 0; i < MAX_DIRECTORY_ENTRIES_PER_BLOCK; ++i)
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

            for (int i = 0; i < MAX_DIRECTORY_ENTRIES_PER_BLOCK; ++i)
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
            for (int i = 0; i < MAX_DIRECTORY_ENTRIES_PER_BLOCK; ++i)
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

        make_new_fcb(&rootDirectory, DEFAULT_DIR_MODE, 1);

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

/**
 * Get file and directory attributes (meta-data).
 * @param [in]  path    - The path of the file whose metadata to retrieve
 * @param [in]  stbuf   - The stat buffer to fill with the metadata
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_getattr(const char *path, struct stat *stbuf)
{
    write_log("myfs_getattr(path=\"%s\")\n", path);

    fcb cur_dir;
    int rc = get_fcb_by_path(path, &cur_dir, NULL, 0);

    if (rc != 0)
        return rc;

    stbuf->st_mode = cur_dir.mode;   /* File mode.  */
    stbuf->st_nlink = 2;             /* Link count.  */
    stbuf->st_uid = cur_dir.uid;     /* User ID of the file's owner.  */
    stbuf->st_gid = cur_dir.gid;     /* Group ID of the file's group. */
    stbuf->st_size = cur_dir.size;   /* Size of file, in bytes.  */
    stbuf->st_atime = cur_dir.atime; /* Time of last access.  */
    stbuf->st_mtime = cur_dir.mtime; /* Time of last modification.  */
    stbuf->st_ctime = cur_dir.ctime; /* Time of last status change.  */

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

    int rc = get_fcb_by_path(path, &currentDirectory, NULL, 0);
    if (rc != 0)
        return rc;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    fcb_iterator iter = make_iterator(&currentDirectory, 0);
    dir_data entries;

    uuid_t blockUUID;

    while (get_next_data_block(&iter, &entries, sizeof(entries), &blockUUID) != NULL)
    {
        for (int i = 0; i < MAX_DIRECTORY_ENTRIES_PER_BLOCK; ++i)
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

    int rc = get_fcb_by_path(path, &fcb, NULL, 0);

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

    char *path_copy = strdup(path);
    char *filename = basename(path_copy);
    fcb cur_dir;
    uuid_t cur_dir_id;
    fcb new_file_fcb;
    uuid_t new_file_fcb_id = {0};

    if (strlen(filename) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    int rc = get_fcb_by_path(path, &cur_dir, &cur_dir_id, 1);
    if (rc != 0)
        return rc;

    // make a new file fcb
    make_new_fcb(&new_file_fcb, mode, 0);
    // generate a new fcb id for the new fcb
    uuid_generate(new_file_fcb_id);

    // store the new fcb into database
    write_to_db(new_file_fcb_id, &new_file_fcb, sizeof(fcb));
    
    // update parent directory entries
    rc = add_fcb_to_dir(&cur_dir, filename, new_file_fcb_id);

    // update its parent directory in database
    write_to_db(cur_dir_id, &cur_dir, sizeof(fcb));
    return rc;
}

/**
 * Update the times (actime, modtime) for a file.
 * @param [in] path - The path of the node whose timestamps to update
 * @param [in] tv   - The values to which the timestamps should be set to.
 * @return 0 if successful, < 0 if an error happened.
 */
static int myfs_utimens(const char *path, struct utimbuf *ubuf)
{
    write_log("myfs_utimens(path=\"%s\")\n", path);

    fcb cur_fcb;
    uuid_t fcb_id;

    // read fcb information from path
    int rc = get_fcb_by_path(path, &cur_fcb, &fcb_id, 0);
    if (rc != 0)
        return rc;

    // set up times
    cur_fcb.mtime = ubuf->modtime;
    cur_fcb.atime = ubuf->actime;

    write_to_db(fcb_id, &cur_fcb, sizeof(fcb));
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
    int rc = get_fcb_by_path(path, &fcb, &fileUUID, 0);

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

    int rc = get_fcb_by_path(path, &fileToResize, &fileUUID, 0);

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

    fcb cur_fcb;
    uuid_t cur_fcb_id;

    int rc = get_fcb_by_path(path, &cur_fcb, &cur_fcb_id, 0);
    if (rc != 0)
        return rc;

    // update mode
    cur_fcb.mode |= mode;

    // update the fcb in the database
    write_to_db(cur_fcb_id, &cur_fcb, sizeof(fcb));

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

    fcb cur_fcb;
    uuid_t cur_fcb_id;

    int rc = get_fcb_by_path(path, &cur_fcb, &cur_fcb_id, 0);
    if (rc != 0)
        return rc;

    // update uid and gid
    cur_fcb.uid = uid;
    cur_fcb.gid = gid;

    // update the fcb in the database
    write_to_db(cur_fcb_id, &cur_fcb, sizeof(fcb));

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

    char *path_copy = strdup(path);
    char *filename = basename(path_copy);

    if (strlen(filename) >= MAX_FILENAME_LENGTH)
        return -ENAMETOOLONG;

    // get fcb of parent directory
    fcb parent_dir; 
    uuid_t parent_dir_id;
    int rc = get_fcb_by_path(path, &parent_dir, &parent_dir_id, 1);
    if (rc != 0)
        return rc;

    // set up a new directory fcb
    fcb new_dir;
    uuid_t new_dir_id = {0};
    make_new_fcb(&new_dir, mode, 1);
    uuid_generate(new_dir_id);

    // write the new dir to the database
    write_to_db(new_dir_id, &new_dir, sizeof(fcb));

    // write the new dir entry to the parent dic
    rc = add_fcb_to_dir(&parent_dir, filename, new_dir_id);

    // update the parent dir in the database
    write_to_db(parent_dir_id, &parent_dir, sizeof(fcb));

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

    // SplitPath pathToRemove = splitPath(path);
    char *path_copy = strdup(path);
    char *filename = basename(path_copy);
    fcb parentDir;

    int rc = get_fcb_by_path(path, &parentDir, NULL, 1);

    if (rc != 0)
    {
        // freeSplitPath(&pathToRemove);
        return rc;
    }

    rc = removeFileFCBinDirectory(&parentDir, filename);

    // freeSplitPath(&pathToRemove);

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

    fcb parent_dir;
    char *path_copy = strdup(path);
    char *filename = basename(path_copy);

    // get fcb of parent directory
    int rc = get_fcb_by_path(path, &parent_dir, NULL, 1);
    if (rc != 0)
        return rc;

    rc = removeDirectoryFCBinDirectory(&parent_dir, filename);

    return rc;
}

static int myfs_rename(const char *from, const char *to)
{
    // write_log("myfs_rename(from=\"%s\", to=\"%s\")", from, to);

    // SplitPath fromPath = splitPath(from);

    // uuid_t sourceDirUUID;
    // fcb sourceDirFCB;

    // int rc = get_fcb_by_path(fromPath.parent, &sourceDirFCB, &sourceDirUUID);
    // if (rc != 0)
    // {
    //     freeSplitPath(&fromPath);
    //     return rc;
    // }
    // uuid_t sourceUUID;
    // fcb sourceFCB;

    // rc = getFCBInDirectory(&sourceDirFCB, fromPath.name, &sourceFCB, &sourceUUID);
    // if (rc != 0)
    // {
    //     freeSplitPath(&fromPath);
    //     return rc;
    // }

    // SplitPath toPath = splitPath(to);

    // uuid_t targetDirUUID = {0};
    // fcb targetParentFCB;
    // fcb *targetParentFCBPtr;

    // if (strcmp(toPath.parent, fromPath.parent) == 0)
    // {
    //     targetParentFCBPtr = &sourceDirFCB;
    //     uuid_copy(targetDirUUID, sourceDirUUID);
    // }
    // else
    // {
    //     targetParentFCBPtr = &targetParentFCB;
    //     rc = get_fcb_by_path(toPath.parent, targetParentFCBPtr, &targetDirUUID, 1);
    //     if (rc != 0)
    //     {
    //         freeSplitPath(&fromPath);
    //         freeSplitPath(&toPath);
    //         return rc;
    //     }
    // }

    // fcb targetFcb;

    // rc = getFCBInDirectory(targetParentFCBPtr, toPath.name, &targetFcb, NULL);

    // if (rc == 0)
    // {
    //     if (S_ISDIR(targetFcb.mode))
    //     {
    //         rc = removeDirectoryFCBinDirectory(targetParentFCBPtr, toPath.name);
    //         if (rc != 0)
    //         {
    //             freeSplitPath(&fromPath);
    //             freeSplitPath(&toPath);
    //             return rc;
    //         }
    //     }
    //     else
    //     {
    //         rc = removeFileFCBinDirectory(targetParentFCBPtr, toPath.name);
    //         if (rc != 0)
    //         {
    //             freeSplitPath(&fromPath);
    //             freeSplitPath(&toPath);
    //             return rc;
    //         }
    //     }

    //     rc = add_fcb_to_dir(targetParentFCBPtr, toPath.name, sourceUUID);
    //     if (rc != 0)
    //     {
    //         freeSplitPath(&fromPath);
    //         freeSplitPath(&toPath);
    //         return rc;
    //     }
    // }
    // else if (rc == -ENOENT)
    // {
    //     rc = add_fcb_to_dir(targetParentFCBPtr, toPath.name, sourceUUID);
    //     if (rc != 0)
    //     {
    //         freeSplitPath(&fromPath);
    //         freeSplitPath(&toPath);
    //         return rc;
    //     }
    // }
    // else
    // {
    //     freeSplitPath(&fromPath);
    //     freeSplitPath(&toPath);
    //     return rc;
    // }

    // rc = unlinkLinkInDirectory(&sourceDirFCB, fromPath.name);
    // if (rc != 0)
    // {
    //     freeSplitPath(&fromPath);
    //     freeSplitPath(&toPath);
    //     return rc;
    // }

    // rc = unqlite_kv_store(pDb, targetDirUUID, KEY_SIZE, targetParentFCBPtr, sizeof(targetParentFCB));
    // error_handler(rc);

    // rc = unqlite_kv_store(pDb, sourceDirUUID, KEY_SIZE, &sourceDirFCB, sizeof(sourceDirFCB));
    // error_handler(rc);

    // freeSplitPath(&fromPath);
    // freeSplitPath(&toPath);
    // return 0;
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
#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>
#include <libgen.h>
#include "myfs.h"

unqlite *pDb;
uuid_t zero_uuid;

/**
 * @param id 	- Given the id, retrieve
 * @param data 	- where returning results will be put into
 * @param size 	- expected size of fetched results
 */
void read_from_db(const uuid_t id, void *data, size_t size)
{
    write_log("read %d \n", id);
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
void write_to_db(const uuid_t id, void *data, size_t size)
{
    write_log("Write Size: %d \n", size);
    int rc = unqlite_kv_store(pDb, id, KEY_SIZE, data, size);
    error_handler(rc);
}

/**
 * Delete item in database based on the id
 * @param id - the key 
 */
void delete_from_db(const uuid_t id)
{
    write_log("delete %d \n", id);
    int rc = unqlite_kv_delete(pDb, id, KEY_SIZE);
    error_handler(rc);
}

/**
 * Make a new fcb for dir or file
 * @param cur_fcb - where to store the new fcb
 * @param mode      - the mode
 * @param is_dir    - 1 for dir 
 */
void make_new_fcb(fcb *cur_fcb, mode_t mode, int is_dir)
{
    memset(cur_fcb, 0, sizeof(fcb));
    if (is_dir)
        cur_fcb->mode = S_IFDIR | mode;
    else
        cur_fcb->mode = S_IFREG | mode;

    time_t now = time(NULL);
    cur_fcb->atime = now;
    cur_fcb->mtime = now;
    cur_fcb->ctime = now;

    cur_fcb->uid = getuid();
    cur_fcb->gid = getgid();

    cur_fcb->count = 1;
}

/**
 * Create an empty data block, write to database, and fill its id into arguments
 * @param block_id - id to be filled with the new data block
 * @param block_size - the size of the new data block
 */
void make_empty_data_block(uuid_t block_id, size_t block_size)
{
    uuid_generate(block_id);                              // generate a new uuid
    char *empty_block = malloc(sizeof(char) * block_size); // create a new empty block
    memset(empty_block, 0, sizeof(block_size));
    write_to_db(block_id, empty_block, sizeof(char) * block_size); // write the new block to database
    free(empty_block);
}

/**
 * Obtain a new iterator over the current fcb. It will iterate over each data blocks the fcb has
 * @param fcb - The fcb whose data blocks will be iterated
 * @param create - 1 for creating a new data block when iterating to an used block
 * @return The iterator contains information about the current status
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
 * Get the root fcb
 * @param root_fcb - where stores the root fcb
 * @return 0 on success, < 0 on failure
 */
int get_root_fcb(fcb *root_fcb)
{
    read_from_db(ROOT_OBJECT_KEY, root_fcb, sizeof(fcb));
    return 0;
}

/**
 * Get the next direct data block
 * @param iterator - the current iterator
 * @param block_size - the size of the data block for potential create_empty_data_block
 * @param next_block_id - filled by the next block id
 * @return 0 on success, < 0 on failure
 */
int get_next_direct_block(fcb_iterator *iterator, size_t block_size, uuid_t next_block_id)
{
    if (iterator->level != 0) // check the level
        return -1;

    // if meeting an unused block
    if (uuid_compare(iterator->fcb->data_blocks[iterator->index], zero_uuid) == 0)
    {
        if (iterator->create)
        { // creation allowed, creating an empty data block
            make_empty_data_block(next_block_id, block_size);
            uuid_copy(iterator->fcb->data_blocks[iterator->index], next_block_id); // update the new id
        }
        else // creation not allowed, indicating that iterating ends
            return -1;
    }
    else // if meeting a used block, directly update the next_block_id
        uuid_copy(next_block_id, iterator->fcb->data_blocks[iterator->index]);

    iterator->index++; // index increments
    if (iterator->index == NUMBER_DIRECT_BLOCKS)
    { // need to move to next level: indirect
        iterator->level += 1;
        iterator->index = 0;
    }
    return 0;
}

/**
 * Get the next data block in indirect level
 * @param iterator - the current iterator
 * @param block_size - the size of the data block for potential create_empty_data_block
 * @param next_block_id - filled by the next block id
 * @return 0 on success, < 0 on failure
 */
int get_next_indirect_block(fcb_iterator *iterator, size_t block_size, uuid_t next_block_id)
{
    if (iterator->level != 1 || iterator->index >= MAX_UUIDS_PER_BLOCK)
        return -1;

    uuid_t blocks[MAX_UUIDS_PER_BLOCK] = {{0}}; // the content of the indirect data block
    unqlite_int64 blocks_size = sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK;

    if (uuid_compare(iterator->fcb->indirect_data_block, zero_uuid) == 0)
    {
        // if creating not allowed, then we already iterated to the end, so return NULL
        if (iterator->create == 0)
            return -1;
        // otherwise, we generate a new indirect data block to hold many uuid_t
        uuid_generate(iterator->fcb->indirect_data_block);
        write_to_db(iterator->fcb->indirect_data_block, *blocks, blocks_size);
    }

    // read content of the indirect data block
    read_from_db(iterator->fcb->indirect_data_block, *blocks, blocks_size);

    if (iterator->create && uuid_compare(blocks[iterator->index], zero_uuid) == 0)
    {
        make_empty_data_block(next_block_id, block_size);
        uuid_copy(blocks[iterator->index], next_block_id);
        write_to_db(iterator->fcb->indirect_data_block, *blocks, blocks_size);
    }
    else
        uuid_copy(next_block_id, blocks[iterator->index]);

    iterator->index++;
    return 0;
}

/**
 * Get next data block. Iterating over direct blocks first and then iterating over blocks in indirect level
 * @param iterator		- The iterator
 * @param block         - where to store the next data block
 * @param block_size    - the size of the data block
 * @param block_id      - the id of the data block
 * @return data block on success, NULL on failure
 */
void *get_next_data_block(fcb_iterator *iterator, void *block, size_t block_size, uuid_t *block_id)
{
    uuid_t next_block_id = {0}; // a temporary variable to store

    if (iterator->level > MAX_INDEX_LEVEL)
        return NULL;

    if (iterator->level == 0) // level is 0: we are iterating over direct blocks
    {
        if (get_next_direct_block(iterator, block_size, next_block_id) < 0)
            return NULL;
    }
    else if (iterator->level == 1) // level is 1: we are iterating over blocks in indirect block
    {
        if (get_next_indirect_block(iterator, block_size, next_block_id) < 0)
            return NULL;
    }
    else
        return NULL;

    // if no valid next block id is retrieved
    if (uuid_compare(next_block_id, zero_uuid) == 0)
        return NULL;

    uuid_copy(*block_id, next_block_id);

    // retrieve the next block to return
    read_from_db(next_block_id, block, block_size);
    return block;
}

/**
 * Get the fcb from its parent dir
 * @param parent_dir - the parent dir fcb
 * @param name - the name of the dir or file we want to find
 * @param cur_fcb - where to store the fetched fcb
 * @param cur_fcb_id - where to store the id of fetched fcb. if NULL, then dont have to store
 * @return 0 on success, < 0 on failure
 */
int get_fcb_from_dir(fcb *parent_dir, const char *name, uuid_t *cur_fcb_id, fcb *cur_fcb)
{
    write_log("get fcb from dir \n");
    if (!S_ISDIR(parent_dir->mode))
        return -ENOTDIR;

    fcb_iterator iterator = make_iterator(parent_dir, 0);
    dir_data parent_dir_data;
    uuid_t parent_dir_data_id;

    while (get_next_data_block(&iterator, &parent_dir_data, sizeof(dir_data), &parent_dir_data_id) != NULL)
    {
        for (int index = 0; index < MAX_DIRECTORY_ENTRIES_PER_BLOCK; index++)
        { // find the filename
            if (strcmp(parent_dir_data.entries[index].name, name) == 0)
            { // retrieve the correct fcb
                read_from_db(parent_dir_data.entries[index].fcb_id, cur_fcb, sizeof(fcb));
                if (cur_fcb_id != NULL)
                    uuid_copy(*cur_fcb_id, parent_dir_data.entries[index].fcb_id);
                return 0;
            }
        }
    }
    return -ENOENT; // no such directory or file
}

/**
 * Get the file control block of the node found at the given path
 * @param path - the path of the target fcb
 * @param cur_fcb_id - where to store the id of fetched fcb
 * @param cur_fcb - where to store the fetched fcb
 * @param get_parent - 1 if we want to get the parent fcb, 0 for the base fcb
 * @return 0 on success, < 0 on failure
 */
int get_fcb_by_path(const char *path, uuid_t *cur_fcb_id, fcb *cur_fcb, int get_parent)
{
    write_log("get fcb by path \n");
    char *path_copy = strdup(path);
    char *path_remaining;

    // if parent fcb wanted, only keep the dir name
    if (get_parent)
        path_copy = dirname(path_copy);
    char *cur_name = strtok_r(path_copy, "/", &path_remaining);

    // get the parent directory fcb from root
    fcb cur_dir;
    get_root_fcb(&cur_dir); // from root fcb to search
    if (cur_fcb_id != NULL)
        uuid_copy(*cur_fcb_id, ROOT_OBJECT_KEY);

    while (cur_name != NULL)
    {
        int rc = get_fcb_from_dir(&cur_dir, cur_name, cur_fcb_id, &cur_dir);
        if (rc != 0)
            return rc;

        cur_name = strtok_r(NULL, "/", &path_remaining); // keep extract the next name
    }

    memcpy(cur_fcb, &cur_dir, sizeof(fcb));
    free(path_copy);

    return 0;
}

/**
 * Add a fcb to its parent dir
 * @param parent_dir_fcb - the fcb of its parent dir
 * @param name - the name of fcb to be added
 * @param fcb_id - the id of fcb to be added
 * @return 0 on success, < 0 on failure
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
        if (parent_dir_data.cur_len == MAX_DIRECTORY_ENTRIES_PER_BLOCK)
            continue;

        for (int index = 0; index < MAX_DIRECTORY_ENTRIES_PER_BLOCK; index++)
        {
            if (strcmp(parent_dir_data.entries[index].name, "") == 0)
            {
                // set up the current empty entry
                uuid_copy(parent_dir_data.entries[index].fcb_id, fcb_id);
                strcpy(parent_dir_data.entries[index].name, name);
                // update the used count
                parent_dir_data.cur_len += 1;

                // update the parent dir data
                write_to_db(data_block_id, &parent_dir_data, sizeof(dir_data));
                return 0;
            }
        }
    }
    return -ENOSPC; // no extra space
}

/**
 * Check if a dir is empty
 * @param cur_dir - the dir to be checked
 * @return 1 on empty, 0 on not empty
 */
int dir_is_empty(fcb *cur_dir)
{
    if (!S_ISDIR(cur_dir->mode))
        return -ENOTDIR;

    fcb_iterator iterator = make_iterator(cur_dir, 0);
    dir_data cur_dir_data;
    uuid_t cur_dir_data_id;

    while (get_next_data_block(&iterator, &cur_dir_data, sizeof(dir_data), &cur_dir_data_id) != NULL)
    {
        if (cur_dir_data.cur_len > 0) // indicates non-empty dir
            return 0;
    }
    return 1;
}

/**
 * Delete a file or dir from its parent dir
 * @param parent_dir - the fcb of its parent dir
 * @param name - the name of fcb to be deleted
 * @param is_dir - 1 if the fcb to be deleted is a dir
 * @param delete_data - 1 if unlink the normal file, 0 for unlinking a hard link
 */
int delete_file_or_dir_from_dir(fcb *parent_dir, const char *name, int is_dir, int delete_data)
{
    if (!S_ISDIR(parent_dir->mode))
        return -ENOTDIR;

    fcb_iterator iterator = make_iterator(parent_dir, 0);
    uuid_t parent_dir_data_id;
    dir_data parent_dir_data;

    while (get_next_data_block(&iterator, &parent_dir_data, sizeof(dir_data), &parent_dir_data_id) != NULL)
    {
        for (int index = 0; index < MAX_DIRECTORY_ENTRIES_PER_BLOCK; index++)
        { // if we find the matching filename
            if (strcmp(parent_dir_data.entries[index].name, name) == 0)
            {
                fcb cur_fcb;
                uuid_t fcb_id;
                uuid_copy(fcb_id, parent_dir_data.entries[index].fcb_id);
                read_from_db(fcb_id, &cur_fcb, sizeof(fcb)); // read fcb content

                if (is_dir) // delete a dir
                {
                    if (!S_ISDIR(cur_fcb.mode))
                        return -ENOTDIR;
                    int empty_dir = dir_is_empty(&cur_fcb);
                    if (empty_dir == 0)
                        return -ENOTEMPTY;
                }
                else
                { // delete a file
                    if (!S_ISREG(cur_fcb.mode))
                        return -EISDIR;
                }
                if (delete_data)
                {
                    // delete the data blocks of the cur_fcb
                    uuid_t data_block_id; // the id of the current data block
                    int size = (is_dir == 1 ? sizeof(dir_data) : sizeof(file_data));
                    char data_block[size]; // the content of the current data block
                    fcb_iterator iter = make_iterator(&cur_fcb, 0);

                    while (get_next_data_block(&iter, &data_block, size, &data_block_id))
                        delete_from_db(data_block_id); // delete them in database
                    delete_from_db(fcb_id);            // delete fcb itself
                }
                else
                {
                    fcb true_fcb;
                    read_from_db(fcb_id, &true_fcb, sizeof(fcb));
                    true_fcb.count -= 1;
                    write_to_db(fcb_id, &true_fcb, sizeof(fcb));
                }

                // update parent dir
                memset(&parent_dir_data.entries[index].fcb_id, 0, sizeof(uuid_t));
                memset(&parent_dir_data.entries[index].name, 0, MAX_FILENAME_LENGTH);
                parent_dir_data.cur_len -= 1;

                // update parent dir in database
                write_to_db(parent_dir_data_id, &parent_dir_data, sizeof(dir_data));

                return 0;
            }
        }
    }
    return -ENOENT;
}

/**
 * Reads data from a file data block
 * @param dest - where to store the read data
 * @param src - where the data come from
 * @param offset - the position where to start read
 * @param size - the number of bytes to be read
 * @return 0 on success, < 0 on failure
 */
int read_block(char *dest, file_data *src, off_t offset, size_t size)
{
    int start = offset;
    int end = min(start + size, BLOCK_SIZE);
    int bytes_read = end - start;
    memcpy(dest, &src->data[offset], bytes_read);
    return bytes_read;
}

/**
 * Write file data to a data block
 * @param dest - the data block where the data written to
 * @param src - the source data buffer
 * @param offset - the position where to start writing
 * @param size - the number of bytes to be written
 * @return 0 on success, < 0 on failure
 */
int write_block(file_data *dest, const char *src, off_t offset, size_t size)
{
    int start = offset;
    int end = min(start + size, BLOCK_SIZE);
    int bytes_written = end - start;
    memcpy(&dest->data[start], src, bytes_written);
    dest->size = start + bytes_written;
    return bytes_written;
}

/**
 * Get file and directory attributes (meta-data)
 * @param path 	- path of the file which is retrieve
 * @param stbuf - stat buffer to be filled with meta-data of the file
 * @return 0 for success, < 0 for failure
 */
static int myfs_getattr(const char *path, struct stat *stbuf)
{
    write_log("myfs_getattr(path=\"%s\")\n", path);

    fcb cur_dir;
    int rc = get_fcb_by_path(path, NULL, &cur_dir, 0);

    if (rc != 0)
        return rc;

    stbuf->st_mode = cur_dir.mode;   /* File mode.  */
    stbuf->st_nlink = cur_dir.count; /* Link count.  */
    stbuf->st_uid = cur_dir.uid;     /* User ID of the file's owner.  */
    stbuf->st_gid = cur_dir.gid;     /* Group ID of the file's group. */
    stbuf->st_size = cur_dir.size;   /* Size of file, in bytes.  */
    stbuf->st_atime = cur_dir.atime; /* Time of last access.  */
    stbuf->st_mtime = cur_dir.mtime; /* Time of last modification.  */
    stbuf->st_ctime = cur_dir.ctime; /* Time of last status change.  */

    return 0;
}

/**
 * Read a directory
 * @param path 		- The path of the directory to be read
 * @param buf 		- Buffer to be passed to the filler()
 * @param filler	- A function used to fill the buffer
 * @param offset	- 
 * @param fi 		- File handler
 * @return 0 on success, < 0 on failure
 */
static int myfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    write_log("myfs_readdir(path=\"%s\")\n", path);

    fcb cur_dir;
    fcb_iterator iterator = make_iterator(&cur_dir, 0);
    dir_data cur_dir_data;
    uuid_t cur_dir_data_id;

    int rc = get_fcb_by_path(path, NULL, &cur_dir, 0);
    if (rc != 0)
        return rc;

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    while (get_next_data_block(&iterator, &cur_dir_data, sizeof(dir_data), &cur_dir_data_id) != NULL)
    {
        for (int index = 0; index < MAX_DIRECTORY_ENTRIES_PER_BLOCK; index++)
        {
            if (strcmp(cur_dir_data.entries[index].name, "") != 0)
            {
                filler(buf, cur_dir_data.entries[index].name, NULL, 0);
            }
        }
    }

    return 0;
}

/**
 * Read data from a file.
 * @param path    - the path of the file to be read
 * @param buf     - the buffer to store the read information
 * @param size    - the number of bytes to be read
 * @param offset  - the position where to start reading
 * @param fi      - file handler
 * @return 0 on success, < 0 on failure
 */
static int myfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    write_log("myfs_read(path=\"%s\", size=%zu, offset=%zu", path, size, offset);
    if (size <= 0)
        return -1;

    fcb cur_fcb;
    int rc = get_fcb_by_path(path, NULL, &cur_fcb, 0);
    if (rc != 0)
        return rc;
    if (!S_ISREG(cur_fcb.mode))
        return -EISDIR; // if not a file

    unsigned bytes_read = 0;
    fcb_iterator iterator = make_iterator(&cur_fcb, 0);
    file_data file_data_block;
    uuid_t file_data_id;
    size_t remaining_size = size;

    while (get_next_data_block(&iterator, &file_data_block, sizeof(file_data), &file_data_id) != NULL)
    {
        if (remaining_size == 0)
            return bytes_read;
        if (offset >= BLOCK_SIZE)
        {
            offset -= BLOCK_SIZE;
            continue;
        }
        off_t cur_bytes_read = read_block(&buf[bytes_read], &file_data_block, offset, remaining_size);
        remaining_size -= cur_bytes_read;
        bytes_read += cur_bytes_read;
        offset = max(0, offset - cur_bytes_read);
    }

    return bytes_read;
}

/**
 * Create a new file
 * @param path - the path where to create a file
 * @param mode - the file mode
 * @param fi - file handler
 * @return 0 on success, < 0 on failure
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

    int rc = get_fcb_by_path(path, &cur_dir_id, &cur_dir, 1);
    if (rc != 0)
        return rc;

    // make a new file fcb
    make_new_fcb(&new_file_fcb, mode, 0);
    // generate a new fcb id for the new fcb
    uuid_generate(new_file_fcb_id);
    // update parent directory entries
    rc = add_fcb_to_dir(&cur_dir, filename, new_file_fcb_id);

    // store the new fcb into database
    write_to_db(new_file_fcb_id, &new_file_fcb, sizeof(fcb));
    // update its parent directory in database
    write_to_db(cur_dir_id, &cur_dir, sizeof(fcb));
    free(path_copy);
    return rc;
}

/**
 * Update the times (actime, modtime) for a file.
 * @param path - the path of the file which needs to be modified
 * @param ubuf - the target timestamps
 * @return 0 on success, < 0 on failure
 */
static int myfs_utimens(const char *path, struct utimbuf *ubuf)
{
    write_log("myfs_utimens(path=\"%s\")\n", path);

    uuid_t fcb_id;
    fcb cur_fcb;
    // read fcb information from path
    int rc = get_fcb_by_path(path, &fcb_id, &cur_fcb, 0);
    if (rc != 0)
        return rc;

    // set up times
    cur_fcb.mtime = ubuf->modtime;
    cur_fcb.atime = ubuf->actime;

    write_to_db(fcb_id, &cur_fcb, sizeof(fcb));
    return 0;
}

/**
 * Write data to a file
 * @param path - the path of file which to be written
 * @param buf  - the data to be written
 * @param offset  - the position where to start writing
 * @param size    - the number of bytes to be written
 * @return 0 on success, < 0 on failure
 */
static int myfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
    if (size <= 0)
        return -1;
    write_log("myfs_write(path=\"%s\", size=%d, offset=%lld,)\n", path, size, offset);

    fcb cur_fcb;
    uuid_t cur_fcb_id;
    int rc = get_fcb_by_path(path, &cur_fcb_id, &cur_fcb, 0);
    if (rc != 0)
        return rc;
    if (!S_ISREG(cur_fcb.mode))
        return -EISDIR;

    int bytes_written = 0;
    off_t original_offset = offset;
    uuid_t cur_data_block_id;
    file_data cur_data_block;
    fcb_iterator iterator = make_iterator(&cur_fcb, 1);

    while (get_next_data_block(&iterator, &cur_data_block, sizeof(file_data), &cur_data_block_id) != NULL)
    {
        if (size == 0)
            break;
        if (offset >= BLOCK_SIZE)
        {
            offset -= BLOCK_SIZE;
            continue;
        }
        int cur_bytes_written = write_block(&cur_data_block, &buf[bytes_written], offset, size);
        offset = max(0, offset - cur_bytes_written);
        size -= cur_bytes_written;
        bytes_written += cur_bytes_written;
        write_to_db(cur_data_block_id, &cur_data_block, sizeof(file_data));
    }

    cur_fcb.size = max(cur_fcb.size, original_offset + bytes_written);
    write_to_db(cur_fcb_id, &cur_fcb, sizeof(fcb));

    return bytes_written;
}

/**
 * Truncate during the direct level
 * @param cur_fcb - the current fcb of the file which needs to be truncated
 * @param remaining_size - the remaining size of the new size
 */
void truncate_direct_level(fcb *cur_fcb, off_t *remaining_size)
{
    for (int index = 0; index < NUMBER_DIRECT_BLOCKS; index++)
    {
        if (*remaining_size != 0)
        {
            if (uuid_compare(cur_fcb->data_blocks[index], zero_uuid) == 0)
            { // if new size > current size, then create an empty block
                uuid_t new_block_id = {0};
                file_data new_data_block;

                new_data_block.size = (int)min(*remaining_size, BLOCK_SIZE);
                *remaining_size -= new_data_block.size;
                write_to_db(new_block_id, &new_data_block, sizeof(file_data));
                uuid_copy(cur_fcb->data_blocks[index], new_block_id);
            }
        }
        else
        { // new size < current size, so delete the following used data blocks
            if (uuid_compare(cur_fcb->data_blocks[index], zero_uuid) != 0)
            {
                delete_from_db(cur_fcb->data_blocks[index]);
                memset(&cur_fcb->data_blocks[index], 0, KEY_SIZE);
            }
        }
    }
}

/**
 * Truncate during the data blocks in indirect level
 * @param cur_fcb - the current fcb of the file which needs to be truncated
 * @param remaining_size - the remaining size of the new size
 */
void truncate_indirect_level(fcb *cur_fcb, off_t *remaining_size)
{
    if (*remaining_size == 0)
    {
        if (uuid_compare(cur_fcb->indirect_data_block, zero_uuid) != 0)
        {
            uuid_t blocks[MAX_UUIDS_PER_BLOCK];
            read_from_db(cur_fcb->indirect_data_block, blocks, sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK);

            for (int index = 0; index < MAX_UUIDS_PER_BLOCK; index++)
            {
                if (uuid_compare(blocks[index], zero_uuid) != 0)
                {
                    delete_from_db(blocks[index]);
                    memset(&blocks[index], 0, KEY_SIZE);
                }
            }
        }
        delete_from_db(cur_fcb->indirect_data_block);
        memset(cur_fcb->indirect_data_block, 0, KEY_SIZE);
    }
    else
    {
        uuid_t blocks[MAX_UUIDS_PER_BLOCK] = {{0}};

        if (uuid_compare(cur_fcb->indirect_data_block, zero_uuid) == 0)
        {
            uuid_t indirect_block_id = {0};
            make_empty_data_block(indirect_block_id, sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK);
            uuid_copy(cur_fcb->indirect_data_block, indirect_block_id);
        }

        read_from_db(cur_fcb->indirect_data_block, &blocks, sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK);

        for (int index = 0; index < MAX_UUIDS_PER_BLOCK; index++)
        {
            if (*remaining_size == 0)
            {
                if (uuid_compare(blocks[index], zero_uuid) != 0)
                {
                    delete_from_db(blocks[index]);
                    memset(&blocks[index], 0, KEY_SIZE);
                }
            }
            else
            {
                if (uuid_compare(blocks[index], zero_uuid) == 0)
                {
                    uuid_t new_block_id = {0};
                    file_data new_block;
                    uuid_generate(new_block_id);

                    new_block.size = min(*remaining_size, BLOCK_SIZE);
                    *remaining_size -= new_block.size;

                    write_to_db(new_block_id, &new_block, sizeof(file_data));
                    uuid_copy(blocks[index], new_block_id);
                }
            }
        }
        write_to_db(cur_fcb->indirect_data_block, &blocks, sizeof(uuid_t) * MAX_UUIDS_PER_BLOCK);
    }
}

/**
 * Set the size of a file. New blocks can be added or used blocks can be deleted
 * @param path     - the path of the file to be truncated
 * @param new_size  - the new size of the file
 * @return 0 on success, < 0 on failure
 */
static int myfs_truncate(const char *path, off_t new_size)
{
    write_log("myfs_truncate(path=\"%s\", new_size=%lld)\n", path, new_size);
    if (new_size > MAX_FILE_SIZE)
        return -EFBIG;

    off_t remaining_size = new_size;
    uuid_t cur_fcb_id;
    fcb cur_fcb;
    int rc = get_fcb_by_path(path, &cur_fcb_id, &cur_fcb, 0);
    if (rc != 0)
        return rc;

    // truncate during the direct level
    truncate_direct_level(&cur_fcb, &remaining_size);
    // truncate during the indirect level
    truncate_indirect_level(&cur_fcb, &remaining_size);

    // update the cur fcb in database
    cur_fcb.size = new_size;
    write_to_db(cur_fcb_id, &cur_fcb, sizeof(fcb));

    return 0;
}

/**
 * Set the permissions of a file
 * @param path - the path of the file whose permissions to be changed
 * @param mode - the new permisison
 * @return 0 on success, < 0 on failure
 */
static int myfs_chmod(const char *path, mode_t mode)
{
    write_log("myfs_chmod(path=\"%s\", mode=0%03o)\n", path, mode);

    fcb cur_fcb;
    uuid_t cur_fcb_id;

    int rc = get_fcb_by_path(path, &cur_fcb_id, &cur_fcb, 0);
    if (rc != 0)
        return rc;

    // update mode
    cur_fcb.mode |= mode;

    // update the fcb in the database
    write_to_db(cur_fcb_id, &cur_fcb, sizeof(fcb));

    return 0;
}

/**
 * Change ownerships of a given file
 * @param path - the path of the file whose owners to be changed
 * @param uid  - the new owning user
 * @param gid  - the new owning group
 * @return 0 on success, < 0 on failure
 */
static int myfs_chown(const char *path, uid_t uid, gid_t gid)
{
    write_log("myfs_chown(path=\"%s\", uid=%d, gid=%d)\n", path, uid, gid);

    fcb cur_fcb;
    uuid_t cur_fcb_id;

    int rc = get_fcb_by_path(path, &cur_fcb_id, &cur_fcb, 0);
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
 * Create a new directory
 * @param path - the path at where to create the new directory
 * @param mode - the permissions of the new directory
 * @return 0 on success, < 0 on failure
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
    int rc = get_fcb_by_path(path, &parent_dir_id, &parent_dir, 1);
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
 * Delete a file
 * @param path - the path of the file to be deleted
 * @return 0 on success, < 0 on failure
 */
static int myfs_unlink(const char *path)
{
    write_log("myfs_unlink(path=\"%s\")\n", path);

    char *path_copy = strdup(path);
    char *filename = basename(path_copy);
    fcb parent_dir;
    fcb cur_fcb;
    uuid_t cur_fcb_id;

    int rc = get_fcb_by_path(path, NULL, &parent_dir, 1);
    if (rc != 0)
        return rc;
    rc = get_fcb_from_dir(&parent_dir, filename, &cur_fcb_id, &cur_fcb);
    if (rc != 0)
        return rc;

    if ((unsigned)cur_fcb.count == (unsigned)1)
        rc = delete_file_or_dir_from_dir(&parent_dir, filename, 0, 1);
    else
    {
        rc = delete_file_or_dir_from_dir(&parent_dir, filename, 0, 0);
    }

    return rc;
}

/**
 * Delete the directory
 * @param path - the path of the directory to be deleted
 * @return 0 on success, < 0 on failure
 */
static int myfs_rmdir(const char *path)
{
    write_log("myfs_rmdir(path=\"%s\")\n", path);

    fcb parent_dir;
    char *path_copy = strdup(path);
    char *filename = basename(path_copy);

    // get fcb of parent directory
    int rc = get_fcb_by_path(path, NULL, &parent_dir, 1);
    if (rc != 0)
        return rc;

    rc = delete_file_or_dir_from_dir(&parent_dir, filename, 1, 1);
    free(path_copy);
    return rc;
}

/**
 * Create a new hard link 
 * @param from - the path of the src file
 * @param to - the path of the new file to be linked
 * @return 0 on success, < 0 on failure
 */
static int myfs_link(const char *from, const char *to)
{
    char *to_copy = strdup(to);
    char *from_copy = strdup(from);
    char *dest_filename = basename(to_copy);
    char *src_filename = basename(from_copy);

    uuid_t dest_parent_fcb_id;
    fcb dest_parent_fcb;
    uuid_t dest_fcb_id;
    fcb dest_fcb;
    // get dest parent dir fcb
    int rc = get_fcb_by_path(to, &dest_parent_fcb_id, &dest_parent_fcb, 1);
    if (rc != 0)
        return rc;
    // get dest fcb itself
    rc = get_fcb_from_dir(&dest_parent_fcb, dest_filename, &dest_fcb_id, &dest_fcb);
    if (rc == 0)
        return -EEXIST;
    if (S_ISDIR(dest_fcb.mode))
        return -EISDIR;

    uuid_t src_fcb_id;
    fcb src_fcb;
    rc = get_fcb_by_path(from, &src_fcb_id, &src_fcb, 0);
    if (rc != 0)
        return rc;
    if (S_ISDIR(src_fcb.mode))
        return -EISDIR;

    // update dest parent dir
    add_fcb_to_dir(&dest_parent_fcb, dest_filename, src_fcb_id);
    write_to_db(dest_parent_fcb_id, &dest_parent_fcb, sizeof(fcb));

    // update src file referencing count and database
    src_fcb.count += 1;
    write_log("links: %d \n", src_fcb.count);
    write_to_db(src_fcb_id, &src_fcb, sizeof(fcb));

    free(to_copy);
    free(from_copy);
    return 0;
}

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
    .link = myfs_link};

/**
 * Initialize the file system before mounting
 */
static void init_fs()
{
    int rc = 0;
    printf("init_fs\n");
	//Initialise the store.
	uuid_clear(zero_uuid);

    rc = unqlite_open(&pDb, DATABASE_NAME, UNQLITE_OPEN_CREATE);
    if (rc != UNQLITE_OK)
        error_handler(rc);

    unqlite_int64 nBytes;
    rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, KEY_SIZE, NULL, &nBytes);

    if (rc == UNQLITE_NOTFOUND)
    {
        perror("init_store: root object was not found\n");
        fcb the_root_fcb;
        mode_t root_mode = S_IFDIR|S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH;
        make_new_fcb(&the_root_fcb, root_mode, 1);
        rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, KEY_SIZE, &the_root_fcb, sizeof(fcb));
        error_handler(rc);
    }
    else
    {
        if(rc==UNQLITE_OK) { 
	 		printf("init_store: root object was found\n"); 
        }
	 	if(nBytes!=sizeof(fcb)) { 
			printf("Data object has unexpected size. Doing nothing.\n");
			exit(-1);
        }
    }
}

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
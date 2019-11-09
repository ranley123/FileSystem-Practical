# CS3104-FileSystem-Practical

## 1 Requirements
1. support for a tree-structured directory hierarchy
2. directories may contain both files and sub-directories
3. file and directory meta-data including size, owner, group, mode and data/meta-data modification times
4. arbitrary length file names (up to system limits)
5. file and directory creation, update and deletion
6. variable length files and directories (you may impose some modest limits)
7. file truncation
8. logging of individual file system operations to file (already implemented in the starter code)

## 2 Guidelines
1. mount the file system: `./myfs /home/ranley/hy30/mnt`
2. unmount the file system `fusermount -u /home/ranley/hy30/mnt`

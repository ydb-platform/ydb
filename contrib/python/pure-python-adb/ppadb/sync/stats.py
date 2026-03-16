# The following constant were extracted from `man 2 stat` on Ubuntu 12.10.
S_IFMT = 0o170000  # bit mask for the file type bit fields
S_IFSOCK = 0o140000  # socket
S_IFLNK = 0o120000  # symbolic link
S_IFREG = 0o100000  # regular file
S_IFBLK = 0o060000  # block device
S_IFDIR = 0o040000  # directory
S_IFCHR = 0o020000  # character device
S_IFIFO = 0o010000  # FIFO
S_ISUID = 0o004000  # set UID bit
S_ISGID = 0o002000  # set-group-ID bit (see below)
S_ISVTX = 0o001000  # sticky bit (see below)
S_IRWXU = 0o0700  # mask for file owner permissions
S_IRUSR = 0o0400  # owner has read permission
S_IWUSR = 0o0200  # owner has write permission
S_IXUSR = 0o0100  # owner has execute permission
S_IRWXG = 0o0070  # mask for group permissions
S_IRGRP = 0o0040  # group has read permission

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/environment.h>
#include <aws/common/file.h>
#include <aws/common/string.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

FILE *aws_fopen_safe(const struct aws_string *file_path, const struct aws_string *mode) {
    return fopen(aws_string_c_str(file_path), aws_string_c_str(mode));
}

static int s_parse_and_raise_error(int errno_cpy) {
    if (errno_cpy == 0) {
        return AWS_OP_SUCCESS;
    }

    if (errno_cpy == ENOENT || errno_cpy == ENOTDIR) {
        return aws_raise_error(AWS_ERROR_FILE_INVALID_PATH);
    }

    if (errno_cpy == EMFILE || errno_cpy == ENFILE) {
        return aws_raise_error(AWS_ERROR_MAX_FDS_EXCEEDED);
    }

    if (errno_cpy == EACCES) {
        return aws_raise_error(AWS_ERROR_NO_PERMISSION);
    }

    if (errno_cpy == ENOTEMPTY) {
        return aws_raise_error(AWS_ERROR_DIRECTORY_NOT_EMPTY);
    }

    return aws_raise_error(AWS_ERROR_UNKNOWN);
}

int aws_directory_create(const struct aws_string *dir_path) {
    int mkdir_ret = mkdir(aws_string_c_str(dir_path), S_IRWXU | S_IRWXG | S_IRWXO);

    /** nobody cares if it already existed. */
    if (mkdir_ret != 0 && errno != EEXIST) {
        return s_parse_and_raise_error(errno);
    }

    return AWS_OP_SUCCESS;
}

bool aws_directory_exists(const struct aws_string *dir_path) {
    struct stat dir_info;
    if (lstat(aws_string_c_str(dir_path), &dir_info) == 0 && S_ISDIR(dir_info.st_mode)) {
        return true;
    }

    return false;
}

static bool s_delete_file_or_directory(const struct aws_directory_entry *entry, void *user_data) {
    (void)user_data;

    struct aws_allocator *allocator = aws_default_allocator();

    struct aws_string *path_str = aws_string_new_from_cursor(allocator, &entry->relative_path);
    int ret_val = AWS_OP_SUCCESS;

    if (entry->file_type & AWS_FILE_TYPE_FILE) {
        ret_val = aws_file_delete(path_str);
    }

    if (entry->file_type & AWS_FILE_TYPE_DIRECTORY) {
        ret_val = aws_directory_delete(path_str, false);
    }

    aws_string_destroy(path_str);
    return ret_val == AWS_OP_SUCCESS;
}

int aws_directory_delete(const struct aws_string *dir_path, bool recursive) {
    if (!aws_directory_exists(dir_path)) {
        return AWS_OP_SUCCESS;
    }

    int ret_val = AWS_OP_SUCCESS;

    if (recursive) {
        ret_val = aws_directory_traverse(aws_default_allocator(), dir_path, true, s_delete_file_or_directory, NULL);
    }

    if (ret_val && aws_last_error() == AWS_ERROR_FILE_INVALID_PATH) {
        aws_reset_error();
        return AWS_OP_SUCCESS;
    }

    if (ret_val) {
        return AWS_OP_ERR;
    }

    int error_code = rmdir(aws_string_c_str(dir_path));

    return error_code == 0 ? AWS_OP_SUCCESS : s_parse_and_raise_error(errno);
}

int aws_directory_or_file_move(const struct aws_string *from, const struct aws_string *to) {
    int error_code = rename(aws_string_c_str(from), aws_string_c_str(to));

    return error_code == 0 ? AWS_OP_SUCCESS : s_parse_and_raise_error(errno);
}

int aws_file_delete(const struct aws_string *file_path) {
    int error_code = unlink(aws_string_c_str(file_path));

    if (!error_code || errno == ENOENT) {
        return AWS_OP_SUCCESS;
    }

    return s_parse_and_raise_error(errno);
}

int aws_directory_traverse(
    struct aws_allocator *allocator,
    const struct aws_string *path,
    bool recursive,
    aws_on_directory_entry *on_entry,
    void *user_data) {
    DIR *dir = opendir(aws_string_c_str(path));

    if (!dir) {
        return s_parse_and_raise_error(errno);
    }

    struct aws_byte_cursor current_path = aws_byte_cursor_from_string(path);
    if (current_path.ptr[current_path.len - 1] == AWS_PATH_DELIM) {
        current_path.len -= 1;
    }

    struct dirent *dirent = NULL;
    int ret_val = AWS_ERROR_SUCCESS;

    errno = 0;
    while (!ret_val && (dirent = readdir(dir)) != NULL) {
        /* note: dirent->name_len is only defined on the BSDs, but not linux. It's not in the
         * required posix spec. So we use dirent->d_name as a c string here. */
        struct aws_byte_cursor name_component = aws_byte_cursor_from_c_str(dirent->d_name);

        if (aws_byte_cursor_eq_c_str(&name_component, "..") || aws_byte_cursor_eq_c_str(&name_component, ".")) {
            continue;
        }

        struct aws_byte_buf relative_path;
        aws_byte_buf_init_copy_from_cursor(&relative_path, allocator, current_path);
        aws_byte_buf_append_byte_dynamic(&relative_path, AWS_PATH_DELIM);
        aws_byte_buf_append_dynamic(&relative_path, &name_component);
        aws_byte_buf_append_byte_dynamic(&relative_path, 0);
        relative_path.len -= 1;

        struct aws_directory_entry entry;
        AWS_ZERO_STRUCT(entry);

        struct stat dir_info;
        if (!lstat((const char *)relative_path.buffer, &dir_info)) {
            if (S_ISDIR(dir_info.st_mode)) {
                entry.file_type |= AWS_FILE_TYPE_DIRECTORY;
            }
            if (S_ISLNK(dir_info.st_mode)) {
                entry.file_type |= AWS_FILE_TYPE_SYM_LINK;
            }
            if (S_ISREG(dir_info.st_mode)) {
                entry.file_type |= AWS_FILE_TYPE_FILE;
                entry.file_size = dir_info.st_size;
            }

            if (!entry.file_type) {
                AWS_ASSERT("Unknown file type encountered");
            }

            entry.relative_path = aws_byte_cursor_from_buf(&relative_path);
            const char *full_path = realpath((const char *)relative_path.buffer, NULL);

            if (full_path) {
                entry.path = aws_byte_cursor_from_c_str(full_path);
            }

            if (recursive && entry.file_type & AWS_FILE_TYPE_DIRECTORY) {
                struct aws_string *rel_path_str = aws_string_new_from_cursor(allocator, &entry.relative_path);
                ret_val = aws_directory_traverse(allocator, rel_path_str, recursive, on_entry, user_data);
                aws_string_destroy(rel_path_str);
            }

            /* post order traversal, if a node below us ended the traversal, don't call the visitor again. */
            if (ret_val && aws_last_error() == AWS_ERROR_OPERATION_INTERUPTED) {
                goto cleanup;
            }

            if (!on_entry(&entry, user_data)) {
                ret_val = aws_raise_error(AWS_ERROR_OPERATION_INTERUPTED);
                goto cleanup;
            }

            if (ret_val) {
                goto cleanup;
            }

        cleanup:
            /* per https://man7.org/linux/man-pages/man3/realpath.3.html, realpath must be freed, if NULL was passed
             * to the second argument. */
            if (full_path) {
                free((void *)full_path);
            }
            aws_byte_buf_clean_up(&relative_path);
        }
    }

    closedir(dir);
    return ret_val;
}

char aws_get_platform_directory_separator(void) {
    return '/';
}

AWS_STATIC_STRING_FROM_LITERAL(s_home_env_var, "HOME");

struct aws_string *aws_get_home_directory(struct aws_allocator *allocator) {

    /* ToDo: check getpwuid_r if environment check fails */
    struct aws_string *home_env_var_value = NULL;
    if (aws_get_environment_value(allocator, s_home_env_var, &home_env_var_value) == 0 && home_env_var_value != NULL) {
        return home_env_var_value;
    }

    return NULL;
}

bool aws_path_exists(const struct aws_string *path) {
    struct stat buffer;
    return stat(aws_string_c_str(path), &buffer) == 0;
}

int aws_fseek(FILE *file, int64_t offset, int whence) {

#ifdef AWS_HAVE_POSIX_LARGE_FILE_SUPPORT
    int result = fseeko(file, offset, whence);
#else
    /* must use fseek(), which takes offset as a long */
    if (offset < LONG_MIN || offset > LONG_MAX) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
    int result = fseek(file, offset, whence);
#endif /* AWS_HAVE_POSIX_LFS */

    if (result != 0) {
        return aws_translate_and_raise_io_error(errno);
    }

    return AWS_OP_SUCCESS;
}

int aws_file_get_length(FILE *file, int64_t *length) {

    struct stat file_stats;

    int fd = fileno(file);
    if (fd == -1) {
        return aws_raise_error(AWS_ERROR_INVALID_FILE_HANDLE);
    }

    if (fstat(fd, &file_stats)) {
        return aws_translate_and_raise_io_error(errno);
    }

    *length = file_stats.st_size;

    return AWS_OP_SUCCESS;
}

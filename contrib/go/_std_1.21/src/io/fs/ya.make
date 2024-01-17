GO_LIBRARY()

SRCS(
    format.go
    fs.go
    glob.go
    readdir.go
    readfile.go
    stat.go
    sub.go
    walk.go
)

GO_XTEST_SRCS(
    example_test.go
    format_test.go
    fs_test.go
    glob_test.go
    readdir_test.go
    readfile_test.go
    stat_test.go
    sub_test.go
    walk_test.go
)

END()

RECURSE(
)

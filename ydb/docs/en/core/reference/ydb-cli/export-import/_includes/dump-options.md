`[options]` – command parameters:

- `-o <PATH>` or `--output <PATH>`: Path to the directory in the client file system where the data will be dumped.
    If the directory doesn't exist, it will be created. However, the entire path to the directory must already exist.

    If the specified directory exists, it must be empty.

    If the parameter is omitted, the `backup_YYYYDDMMTHHMMSS` directory will be created in the current directory, where `YYYYDDMM` is the date and `HHMMSS` is the time when the dump process began, accroding to the system clock.

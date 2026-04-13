# Using the script

1. Publish YDB documentation from a stable release locally:

    ```shell
    yfm -i ~/Projects/ydb/ydb/docs/ -o ~/tmp-docs --allowHTML --apply-presets --quiet 2>&1 | GREP -v '^INFO'
    ```

2. Edit the script settings in the `settings.ini` file.

3. Run `main.py`.

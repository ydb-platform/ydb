this_script_path=`realpath $0`
this_script_dir=`dirname $this_script_path`
#    --summary_links /home/kirrysin/fork/ydb/.github/scripts/tests/debug/summary_links.txt --public_dir /home/kirrysin/fork/ydb/.github/scripts/tests/debug/ --public_dir_url https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/10058257094/ya-x86-64 --build_preset relwithdebinfo --status_report_file statusrep.txt Tests ya-test.html /home/kirrysin/fork/ydb/.github/scripts/tests/debug/junit.xml

  $this_script_dir/../generate-summary.py \
    --summary_links $this_script_dir/summary_links.txt --public_dir $this_script_dir --public_dir_url https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/10058257094/ya-x86-64 --build_preset relwithdebinfo --status_report_file statusrep.txt Tests ya-test.html $this_script_dir/junit.xml



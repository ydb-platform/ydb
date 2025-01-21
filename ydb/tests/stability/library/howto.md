# How to test stability 
1) build ydbd (not stripped)
    ```
    ./ya make --build=profile -DCFLAGS=-fno-omit-frame-pointer --thinlto ydb/apps/ydbd
    ```
2) build library
    ```
    ./ya make  /ydb/tests/stability/library
    ```
3) deploy ydb to test specific build version
    ```
    cd /ydb/tests/stability/library; ./library deploy_ydb --cluster_path=<path/to/>cluster.yaml --ydbd_path=<path/to/>ydb/apps/ydbd/ydbd
    ```
4) deploy tools 
    ```
    ./library deploy_tools --cluster_path=<path/to/>cluster.yaml --ydbd_path=<path/to/>ydb/apps/ydbd/ydbd
    ```
5) start workload:
    - `start_all_workloads` - it start all listed bellow worloads 
    - `start_workload_simple_queue_row` - create
    - `start_workload_simple_queue_column`
    - `start_workload_olap_workload`

    ```
    ./library start_all_workloads --cluster_path=<path/to/>cluster.yaml --ydbd_path=<path/to/>ydb/apps/ydbd/ydbd
    ```
    to stop workload, use command `stop_workloads` - stops all worloads
    
    to check is it working on node host
    - workload simple_queue row  - ``ps -aux | grep "/Berkanavt/nemesis/bin/simple" | grep row | grep -v grep
    ``
    - workload simple_queue column  - ``ps -aux | grep "/Berkanavt/nemesis/bin/simple" | grep column | grep -v grep
    ``
    - workload simple_queue column  - ``ps -aux | grep "/Berkanavt/nemesis/bin/olap_workload" | grep -v grep
    ``

6) start nemesis:
    ```
    ./library start_nemesis --cluster_path=<path/to/>cluster.yaml --ydbd_path=<path/to/>ydb/apps/ydbd/ydbd
    ```
    to stop, use the command `stop_nemesis`


7) Check states
    1) yq to get all node hosts
        ```
        sudo wget https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -O /usr/local/bin/yq
        sudo chmod +x /usr/local/bin/yq
        ```
        ``yq e '.hosts[].name' <path/to/>cluster.yaml> > hosts.txt``
    2) Get status of nemesis and workloads (ad-hoc)
        ```
        parallel-ssh -h hosts.txt -i '
        if systemctl is-active --quiet nemesis; then
            echo "nemesis: Active"
        else
            echo "nemesis: Down"
        fi
        if ps aux | grep "/Berkanavt/nemesis/bin/olap_workload" | grep -v grep > /dev/null; then
            echo "olap_workload: Running"
        else
            echo "olap_workload: Stopped"
        fi
        if ps aux | grep "/Berkanavt/nemesis/bin/simple" | grep column | grep -v grep > /dev/null; then
            echo "simple_queue_column: Running"
        else
            echo "simple_queue_column: Stopped"
        fi
        if ps aux | grep "/Berkanavt/nemesis/bin/simple" | grep row | grep -v grep > /dev/null; then
            echo "simple_queue_column: Running"
        else
            echo "simple_queue_column: Stopped"
        fi
        '
        ```
8) check cluster stability
    1) ``perform_check`` - return summary of errors and coredumps for cluster:

        ```
        SAFETY WARDEN (total: 8)
        LIVENESS WARDEN (total: 0)
        COREDUMPS:
            ydb-sas-testing-0000.search.yandex.net: 1
            ydb-sas-testing-0001.search.yandex.net: 0
            ydb-sas-testing-0002.search.yandex.net: 1
            ydb-sas-testing-0003.search.yandex.net: 1
            ydb-sas-testing-0004.search.yandex.net: 0
            ydb-sas-testing-0005.search.yandex.net: 1
            ydb-sas-testing-0006.search.yandex.net: 2
            ydb-sas-testing-0007.search.yandex.net: 0
        ```
        to run:
        ```
        ./library perform_check --cluster_path=<path/to/>cluster.yaml --ydbd_path=<path/to/>ydb/apps/ydbd/ydbd
        ``` 
    2) get cluster traces (ad-hoc)
        ```
        '' >  combined_traces.txt; parallel-ssh -h hosts.txt -i "
            zgrep -E 'VERIFY|FAIL|signal 11|signal 6|signal 15|uncaught exception' /Berkanavt/kikimr_31003/logs/kikimr.start.* -A 30 | 
            awk '
            {      
                split(\$0, parts, \":\")
                curr_file = parts[1]

                if (curr_file != prev_file) {
                    if (prev_file != \"\")
                        print \"\n\n\n---\n\n\n\"
                    prev_file = curr_file
                }      
                print
            }' | sed '/--/a\\n\n'
            " >> combined_traces.txt
        ```
9) create issue in github about new traces
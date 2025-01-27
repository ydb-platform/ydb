# How to test stability 
1) build ydbd (not stripped)
    ```
    ./ya make -r ydb/apps/ydbd
    ```
2) build library
    ```
    ./ya make -r /ydb/tests/stability/library
    ```
3) deploy ydb
    ```
    cd ydb/tests/stability/library; ./library deploy_ydb --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
    ```
4) deploy tools 
    ```
    ./library deploy_tools --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
    ```
5) start workload:
    - `start_all_workloads` - start all listed below workloads 
    - `start_workload_simple_queue_row`
    - `start_workload_simple_queue_column`
    - `start_workload_olap_workload`
    
    not included in  `start_all_workloads`:
    - `start_workload_log`

    ```
    ./library start_all_workloads --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
    ```
    to stop workload, use command `stop_workloads`
    
    to check is it working on node host
    - workload simple_queue row  - ``ps -aux | grep "/Berkanavt/nemesis/bin/simple" | grep row | grep -v grep
    ``
    - workload simple_queue column  - ``ps -aux | grep "/Berkanavt/nemesis/bin/simple" | grep column | grep -v grep
    ``
    - workload simple_queue column  - ``ps -aux | grep "/Berkanavt/nemesis/bin/olap_workload" | grep -v grep
    ``

6) start nemesis:
    ```
    ./library start_nemesis --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
    ```
    to stop, use the command `stop_nemesis`

7) check states of workloads and nemesis from your host (ad-hoc)
    1) prepare list of cluster node hosts

        ``pip install yq parallel-ssh``

        ``yq '.hosts[].name' <path_to_cluster.yaml> > ~/hosts.txt``
    2) check status
        ```
        parallel-ssh -h ~/hosts.txt -i -x "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no" '
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
        if ps aux | grep "/Berkanavt/nemesis/bin/ydb_cli" | grep -v grep > /dev/null; then
            echo "log workload: Running"
        else
            echo "log workload: Stopped"
        fi
        '
        ```
8) check cluster stability
    1) ``perform_checks`` - return summary of errors and coredumps for cluster:

        ```
        SAFETY WARDEN (total: 8)
        LIVENESS WARDEN (total: 0)
        COREDUMPS:
            node_host_1: 1
            node_host_2: 0
            node_host_3: 1
            node_host_4: 4
        ```
        to run:
        ```
        ./library perform_checks --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
        ``` 
    2) get unique traces

        ```
         ./library get_errors --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
        ```
9) create issue in github about new traces
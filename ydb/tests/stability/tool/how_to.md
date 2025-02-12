# How to test stability 
1) build ydbd (not stripped)
    ```
    ./ya make -r ydb/apps/ydbd
    ```
2) build library
    ```
    ./ya make -r /ydb/tests/stability/tool
    ```
3) deploy ydb
    ```
    cd ydb/tests/stability/tool; ./tool deploy_ydb --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
    ```
4) deploy tools 
    ```
    ./tool deploy_tools --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
    ```
5) start workload:
    - `start_default_workloads` - start all listed below workloads 
    - `start_workload_simple_queue_row`
    - `start_workload_simple_queue_column`
    - `start_workload_olap_workload`
    
    not included in  `start_default_workloads`:
    - `start_workload_log`

    ```
    ./tool start_default_workloads --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
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
    ./tool start_nemesis --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
    ```
    to stop, use the command `stop_nemesis`

7) check states of workloads and nemesis
    ```
    ./tool get_state --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
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
        ./tool perform_checks --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
        ``` 
    2) get unique traces

        ```
         ./tool get_errors --cluster_path=<path_to_cluster.yaml> --ydbd_path=<repo_root>/ydb/apps/ydbd/ydbd
        ```
9) create issue in github about new traces
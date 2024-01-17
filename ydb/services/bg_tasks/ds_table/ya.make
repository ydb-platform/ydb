LIBRARY()

SRCS(
    behaviour.cpp
    executor.cpp
    interrupt.cpp
    task_executor_controller.cpp
    executor_controller.cpp
    task_executor.cpp
    finish_task.cpp
    assign_tasks.cpp
    fetch_tasks.cpp
    config.cpp
    add_tasks.cpp
    task_enabled.cpp
    lock_pinger.cpp
    initialization.cpp
)

PEERDIR(
    ydb/library/accessor
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/services/bg_tasks/abstract
    ydb/services/metadata/initializer
    ydb/core/base
    ydb/services/metadata/request
)

END()

from .. import uwsgi


connect = uwsgi.async_connect

sleep = uwsgi.async_sleep

suspend = uwsgi.suspend

get_loop_name = uwsgi.loop

wait_for_fd_read = uwsgi.wait_fd_read

wait_for_fd_write = uwsgi.wait_fd_write

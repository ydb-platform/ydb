# Todo: rework this file, it is not up to date
# Smoke and Integration tests

## Test for lost client connection:

1. start example.py with log_level='trace'
2. curl http://localhost:8000/endless
3. kill curl

### expected outcome:
all streaming stops, including pings (log output)


## Test for uvicorn shutdown (Ctrl-C) with long running task and multiple clients:
- see also: `test_multiple_consumers.py`

1. start example.py with log_level='trace'
2. curl http://localhost:8000/endless from multiple clients/terminals
3. CTRL-C: stop server

### expected outcome:
1. server shut down gracefully, no pending tasks
2. all clients stop (transfer closed with outstanding read data remaining)


## Test for stream_generator.py
1. start stream_generator.py with log_level='trace'
2. Run http calls in integration_testing.http

### expected outcome:
1. every post from one client should be seen on consuming client

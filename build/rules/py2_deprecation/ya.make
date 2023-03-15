OWNER(g:ymake)
RESOURCES_LIBRARY()

MESSAGE(WARNING You are using deprecated Python2-only dependencies in your code. Please consider rewriting it to Python 3. To list all such errors use `ya make -DFAIL_PY2` or `ya make -ttt -DFAIL_PY2` if you see this error during tests.)

END()

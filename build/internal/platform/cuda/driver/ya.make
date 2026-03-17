LIBRARY()

NO_RUNTIME()

LDFLAGS(-lcuda)

END()

RECURSE(
    implib
    nvidia-ml_implib
)

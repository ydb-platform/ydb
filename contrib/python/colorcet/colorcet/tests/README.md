## Matplotlib tests

These tests contain baseline images generated using [pytest-mpl](https://github.com/matplotlib/pytest-mpl).
To run these tests with the fig checking enabled first install pytest-mpl:

```bash
pip install pytest-mpl
```

To regenerate these figures from within this dir run:

```bash
pytest --mpl-generate-path=baseline
```

To run the tests checking that the output is as expected run:

```bash
pytest --mpl
```

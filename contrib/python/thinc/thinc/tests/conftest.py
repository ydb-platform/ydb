import pytest
from hypothesis import settings

# Functionally disable deadline settings for tests
# to prevent spurious test failures in CI builds.
settings.register_profile("no_deadlines", deadline=2 * 60 * 1000)  # in ms
settings.load_profile("no_deadlines")


def pytest_sessionstart(session):
    # If Tensorflow is installed, attempt to enable memory growth
    # to prevent it from allocating all of the GPU's free memory
    # to its internal memory pool(s).
    try:
        import tensorflow as tf

        physical_devices = tf.config.list_physical_devices("GPU")
        for device in physical_devices:
            try:
                tf.config.experimental.set_memory_growth(device, True)
            except:
                # Invalid device or cannot modify virtual devices once initialized.
                print(f"failed to enable Tensorflow memory growth on {device}")
    except ImportError:
        pass


def pytest_addoption(parser):
    try:
        parser.addoption("--slow", action="store_true", help="include slow tests")
    # Options are already added, e.g. if conftest is copied in a build pipeline
    # and runs twice
    except ValueError:
        pass


def pytest_runtest_setup(item):
    def getopt(opt):
        # When using 'pytest --pyargs thinc' to test an installed copy of
        # thinc, pytest skips running our pytest_addoption() hook. Later, when
        # we call getoption(), pytest raises an error, because it doesn't
        # recognize the option we're asking about. To avoid this, we need to
        # pass a default value. We default to False, i.e., we act like all the
        # options weren't given.
        return item.config.getoption(f"--{opt}", False)

    for opt in ["slow"]:
        if opt in item.keywords and not getopt(opt):
            pytest.skip(f"need --{opt} option to run")


@pytest.fixture()
def pathy_fixture():
    pytest.importorskip("pathy")
    import shutil
    import tempfile

    from pathy import Pathy, use_fs

    temp_folder = tempfile.mkdtemp(prefix="thinc-pathy")
    use_fs(temp_folder)

    root = Pathy("gs://test-bucket")
    root.mkdir(exist_ok=True)

    yield root
    use_fs(False)
    shutil.rmtree(temp_folder)

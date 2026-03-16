import os

ALLOWED_FILE_EXTENSIONS = ["tds", "tdsx", "tde", "hyper", "parquet"]

BYTES_PER_MB = 1024 * 1024

DELAY_SLEEP_SECONDS = 0.1


class Config:
    # The maximum size of a file that can be published in a single request is 64MB
    @property
    def FILESIZE_LIMIT_MB(self):
        return min(int(os.getenv("TSC_FILESIZE_LIMIT_MB", 64)), 64)

    # For when a datasource is over 64MB, break it into 5MB(standard chunk size) chunks
    @property
    def CHUNK_SIZE_MB(self):
        return int(os.getenv("TSC_CHUNK_SIZE_MB", 5 * 10))  # 5MB felt too slow, upped it to 50

    # Default page size
    @property
    def PAGE_SIZE(self):
        return int(os.getenv("TSC_PAGE_SIZE", 100))


config = Config()

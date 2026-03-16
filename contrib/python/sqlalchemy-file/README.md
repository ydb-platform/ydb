# sqlalchemy-file


**SQLAlchemy-file** is a [SQLAlchemy](https://www.sqlalchemy.org/) extension for attaching files to SQLAlchemy model and
uploading them to various storage such as Local Storage, Amazon S3, Rackspace CloudFiles, Google Storage and others
using [Apache Libcloud](https://github.com/apache/libcloud).

<p align="center">
<a href="https://github.com/jowilf/sqlalchemy-file/actions">
    <img src="https://github.com/jowilf/sqlalchemy-file/actions/workflows/test.yml/badge.svg" alt="Test suite">
</a>
<a href="https://github.com/jowilf/sqlalchemy-file/actions">
    <img src="https://github.com/jowilf/sqlalchemy-file/actions/workflows/publish.yml/badge.svg" alt="Publish">
</a>
<a href="https://codecov.io/gh/jowilf/sqlalchemy-file">
    <img src="https://codecov.io/gh/jowilf/sqlalchemy-file/branch/main/graph/badge.svg" alt="Codecov">
</a>
<a href="https://pypi.org/project/sqlalchemy-file/">
    <img src="https://badge.fury.io/py/sqlalchemy-file.svg" alt="Package version">
</a>
<a href="https://pypi.org/project/sqlalchemy-file/">
    <img src="https://img.shields.io/pypi/pyversions/sqlalchemy-file?color=2334D058" alt="Supported Python versions">
</a>
</p>


The key features are:

* **Multiple Storage :** Use Object Storage API provided by [Apache Libcloud](https://github.com/apache/libcloud) to
  store files. Therefore, you can store your files on Local Storage, Amazon S3, Google Cloud Storage, MinIO etc, and
  easily switch between them. For a full list of supported providers
  visit [supported providers page](https://libcloud.readthedocs.io/en/stable/storage/supported_providers.html) from Apache
  Libcloud documentation.
* **Validator :**  Provide an interface for validating each files before saving them.
* **Size Validator :** Built-in validator for file maximum `size` validation.
* **Content-Type Validator :** Built-in validator for file ``mimetype`` restrictions.
* **Image Validator :** Built-in validator for image `mimetype`, `width`, `height` and `ratio` validation.
* **Processor :** Provide an interface to easily save multiple transformation of the original files.
* **ThumbnailGenerator :** Built-in processor to auto generate thumbnail
* **Multiple Files :** You can attach multiple files directly to a Model.
* **Session awareness :** Whenever an object is deleted or a rollback is performed the files uploaded during the unit of
  work or attached to the deleted objects are automatically deleted.
* **Meant for Evolution :** Change the storage provider anytime you want, old data will continue to work
* **SQLModel Support:** Tested with [SQLModel](https://github.com/tiangolo/sqlmodel)

---

**Documentation**: [https://jowilf.github.io/sqlalchemy-file](https://jowilf.github.io/sqlalchemy-file/)

**Source Code**: [https://github.com/jowilf/sqlalchemy-file](https://github.com/jowilf/sqlalchemy-file)

---

## Requirements

A recent and currently supported version of Python (right
now, <a href="https://www.python.org/downloads/" class="external-link" target="_blank">Python supports versions 3.7 and
above</a>).

As **SQLAlchemy-file** is based on **Apache Libcloud** and **SQLAlchemy**, it requires them. They will be automatically
installed when you install SQLAlchemy-file.

## Installation

### PIP

```shell
$ pip install sqlalchemy-file
```

### Poetry

```shell
$ poetry add sqlalchemy-file
```

## Example

Attaching files to models is as simple as declaring a field on the model itself

```Python
import os

from libcloud.storage.drivers.local import LocalStorageDriver
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy_file import File, FileField
from sqlalchemy_file.storage import StorageManager

Base = declarative_base()


# Define your model
class Attachment(Base):
    __tablename__ = "attachment"

    id = Column(Integer, autoincrement=True, primary_key=True)
    name = Column(String(50), unique=True)
    content = Column(FileField)


# Configure Storage
os.makedirs("./upload_dir/attachment", 0o777, exist_ok=True)
container = LocalStorageDriver("./upload_dir").get_container("attachment")
StorageManager.add_storage("default", container)

# Save your model
engine = create_engine(
    "sqlite:///example.db", connect_args={"check_same_thread": False}
)
Base.metadata.create_all(engine)

with Session(engine) as session, open("./example.txt", "rb") as local_file:
    # from an opened local file
    session.add(Attachment(name="attachment1", content=local_file))

    # from bytes
    session.add(Attachment(name="attachment2", content=b"Hello world"))

    # from string
    session.add(Attachment(name="attachment3", content="Hello world"))

    # from a File object with custom filename and content_type
    file = File(content="Hello World", filename="hello.txt", content_type="text/plain")
    session.add(Attachment(name="attachment4", content=file))

    # from a File object specifying a content path
    session.add(Attachment(name="attachment5", content=File(content_path="./example.txt")))

    session.commit()


```

## Related projects and inspirations

* [filedepot: ](https://github.com/amol-/depot) When I was looking for a library like this, depot was the
best I saw. This project inspired **SQLAlchemy-file** extensively
and some features are implemented the same.
* [sqlalchemy-media: ](https://github.com/pylover/sqlalchemy-media) Another attachment extension for SqlAlchemy
to manage assets which are associated with database models

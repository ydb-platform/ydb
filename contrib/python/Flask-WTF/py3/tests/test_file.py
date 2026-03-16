import pytest
from werkzeug.datastructures import FileStorage
from werkzeug.datastructures import ImmutableMultiDict
from werkzeug.datastructures import MultiDict
from wtforms import FileField as BaseFileField
from wtforms import MultipleFileField as BaseMultipleFileField
from wtforms.validators import Length

from flask_wtf import FlaskForm
from flask_wtf.file import FileAllowed
from flask_wtf.file import FileField
from flask_wtf.file import FileRequired
from flask_wtf.file import FileSize
from flask_wtf.file import MultipleFileField


@pytest.fixture
def form(req_ctx):
    class UploadForm(FlaskForm):
        class Meta:
            csrf = False

        file = FileField()
        files = MultipleFileField()

    return UploadForm


def test_process_formdata(form):
    assert form(MultiDict((("file", FileStorage()),))).file.data is None
    assert (
        form(MultiDict((("file", FileStorage(filename="real")),))).file.data is not None
    )


def test_file_required(form):
    form.file.kwargs["validators"] = [FileRequired()]

    f = form()
    assert not f.validate()
    assert f.file.errors[0] == "This field is required."

    f = form(file="not a file")
    assert not f.validate()
    assert f.file.errors[0] == "This field is required."

    f = form(file=FileStorage())
    assert not f.validate()

    f = form(file=FileStorage(filename="real"))
    assert f.validate()


def test_file_allowed(form):
    form.file.kwargs["validators"] = [FileAllowed(("txt",))]

    f = form()
    assert f.validate()

    f = form(file=FileStorage(filename="test.txt"))
    assert f.validate()

    f = form(file=FileStorage(filename="test.png"))
    assert not f.validate()
    assert f.file.errors[0] == "File does not have an approved extension: txt"


def test_file_allowed_uploadset(app, form):
    pytest.importorskip("flask_uploads")
    from flask_uploads import configure_uploads
    from flask_uploads import UploadSet

    app.config["UPLOADS_DEFAULT_DEST"] = "uploads"
    txt = UploadSet("txt", extensions=("txt",))
    configure_uploads(app, (txt,))
    form.file.kwargs["validators"] = [FileAllowed(txt)]

    f = form()
    assert f.validate()

    f = form(file=FileStorage(filename="test.txt"))
    assert f.validate()

    f = form(file=FileStorage(filename="test.png"))
    assert not f.validate()
    assert f.file.errors[0] == "File does not have an approved extension."


def test_file_size_no_file_passes_validation(form):
    form.file.kwargs["validators"] = [FileSize(max_size=100)]
    f = form()
    assert f.validate()


def test_file_size_small_file_passes_validation(form, tmp_path):
    form.file.kwargs["validators"] = [FileSize(max_size=100)]
    path = tmp_path / "test_file_smaller_than_max.txt"
    path.write_bytes(b"\0")

    with path.open("rb") as file:
        f = form(file=FileStorage(file))
        assert f.validate()


@pytest.mark.parametrize(
    "min_size, max_size, invalid_file_size", [(1, 100, 0), (0, 100, 101)]
)
def test_file_size_invalid_file_size_fails_validation(
    form, min_size, max_size, invalid_file_size, tmp_path
):
    form.file.kwargs["validators"] = [FileSize(min_size=min_size, max_size=max_size)]
    path = tmp_path / "test_file_invalid_size.txt"
    path.write_bytes(b"\0" * invalid_file_size)

    with path.open("rb") as file:
        f = form(file=FileStorage(file))
        assert not f.validate()
        assert (
            f.file.errors[0] == f"File must be between {min_size} and {max_size} bytes."
        )


def test_validate_base_field(req_ctx):
    class F(FlaskForm):
        class Meta:
            csrf = False

        f = BaseFileField(validators=[FileRequired()])

    assert not F().validate()
    assert not F(f=FileStorage()).validate()
    assert F(f=FileStorage(filename="real")).validate()
    assert F(f=FileStorage(filename="real")).validate()


def test_process_formdata_for_files(form):
    assert (
        form(
            ImmutableMultiDict([("files", FileStorage()), ("files", FileStorage())])
        ).files.data
        is None
    )
    assert (
        form(
            ImmutableMultiDict(
                [
                    ("files", FileStorage(filename="a.jpg")),
                    ("files", FileStorage(filename="b.jpg")),
                ]
            )
        ).files.data
        is not None
    )


def test_files_required(form):
    form.files.kwargs["validators"] = [FileRequired()]

    f = form()
    assert not f.validate()
    assert f.files.errors[0] == "This field is required."

    f = form(files="not a file")
    assert not f.validate()
    assert f.files.errors[0] == "This field is required."

    f = form(files=[FileStorage()])
    assert not f.validate()

    f = form(files=[FileStorage(filename="real")])
    assert f.validate()


def test_files_allowed(form):
    form.files.kwargs["validators"] = [FileAllowed(("txt",))]

    f = form()
    assert f.validate()

    f = form(
        files=[FileStorage(filename="test.txt"), FileStorage(filename="test2.txt")]
    )
    assert f.validate()

    f = form(files=[FileStorage(filename="test.txt"), FileStorage(filename="test.png")])
    assert not f.validate()
    assert f.files.errors[0] == "File does not have an approved extension: txt"


def test_files_allowed_uploadset(app, form):
    pytest.importorskip("flask_uploads")
    from flask_uploads import configure_uploads
    from flask_uploads import UploadSet

    app.config["UPLOADS_DEFAULT_DEST"] = "uploads"
    txt = UploadSet("txt", extensions=("txt",))
    configure_uploads(app, (txt,))
    form.files.kwargs["validators"] = [FileAllowed(txt)]

    f = form()
    assert f.validate()

    f = form(
        files=[FileStorage(filename="test.txt"), FileStorage(filename="test2.txt")]
    )
    assert f.validate()

    f = form(files=[FileStorage(filename="test.txt"), FileStorage(filename="test.png")])
    assert not f.validate()
    assert f.files.errors[0] == "File does not have an approved extension."


def test_validate_base_multiple_field(req_ctx):
    class F(FlaskForm):
        class Meta:
            csrf = False

        f = BaseMultipleFileField(validators=[FileRequired()])

    assert not F().validate()
    assert not F(f=[FileStorage()]).validate()
    assert F(f=[FileStorage(filename="real")]).validate()


def test_file_size_small_files_pass_validation(form, tmp_path):
    form.files.kwargs["validators"] = [FileSize(max_size=100)]
    path = tmp_path / "test_file_smaller_than_max.txt"
    path.write_bytes(b"\0")

    with path.open("rb") as file:
        f = form(files=[FileStorage(file)])
        assert f.validate()


@pytest.mark.parametrize(
    "min_size, max_size, invalid_file_size", [(1, 100, 0), (0, 100, 101)]
)
def test_file_size_invalid_file_sizes_fails_validation(
    form, min_size, max_size, invalid_file_size, tmp_path
):
    form.files.kwargs["validators"] = [FileSize(min_size=min_size, max_size=max_size)]
    path = tmp_path / "test_file_invalid_size.txt"
    path.write_bytes(b"\0" * invalid_file_size)

    with path.open("rb") as file:
        f = form(files=[FileStorage(file)])
        assert not f.validate()
        assert (
            f.files.errors[0]
            == f"File must be between {min_size} and {max_size} bytes."
        )


def test_files_length(form, min_num=2, max_num=3):
    form.files.kwargs["validators"] = [Length(min_num, max_num)]

    f = form(files=[FileStorage("1")])
    assert not f.validate()
    assert (
        f.files.errors[0]
        == f"Field must be between {min_num} and {max_num} characters long."
    )

    f = form(
        files=[
            FileStorage(filename="1"),
            FileStorage(filename="2"),
        ]
    )
    assert f.validate()

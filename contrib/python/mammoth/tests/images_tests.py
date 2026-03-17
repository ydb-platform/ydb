import io

from precisely import assert_that, has_attrs, is_sequence

import mammoth


def test_inline_is_available_as_alias_of_img_element():
    assert mammoth.images.inline is mammoth.images.img_element


def test_data_uri_encodes_images_in_base64():
    image_bytes = b"abc"
    image = mammoth.documents.Image(
        alt_text=None,
        content_type="image/jpeg",
        open=lambda: io.BytesIO(image_bytes),
    )

    result = mammoth.images.data_uri(image)

    assert_that(result, is_sequence(
        has_attrs(attributes={"src": "data:image/jpeg;base64,YWJj"}),
    ))


class ImgElementTests:
    def test_when_element_does_not_have_alt_text_then_alt_attribute_is_not_set(self):
        image_bytes = b"abc"
        image = mammoth.documents.Image(
            alt_text=None,
            content_type="image/jpeg",
            open=lambda: io.BytesIO(image_bytes),
        )

        @mammoth.images.img_element
        def convert_image(image):
            return {"src": "<src>"}

        result = convert_image(image)

        assert_that(result, is_sequence(
            has_attrs(attributes={"src": "<src>"}),
        ))

    def test_when_element_se_alt_text_then_alt_attribute_is_set(self):
        image_bytes = b"abc"
        image = mammoth.documents.Image(
            alt_text="<alt>",
            content_type="image/jpeg",
            open=lambda: io.BytesIO(image_bytes),
        )

        @mammoth.images.img_element
        def convert_image(image):
            return {"src": "<src>"}

        result = convert_image(image)

        assert_that(result, is_sequence(
            has_attrs(attributes={"alt": "<alt>", "src": "<src>"}),
        ))

    def test_image_alt_text_can_be_overridden_by_alt_attribute_returned_from_function(self):
        image_bytes = b"abc"
        image = mammoth.documents.Image(
            alt_text="<alt>",
            content_type="image/jpeg",
            open=lambda: io.BytesIO(image_bytes),
        )

        @mammoth.images.img_element
        def convert_image(image):
            return {"alt": "<alt override>", "src": "<src>"}

        result = convert_image(image)

        assert_that(result, is_sequence(
            has_attrs(attributes={"alt": "<alt override>", "src": "<src>"}),
        ))

[![Build Status](https://travis-ci.org/yohanboniface/falcon-multipart.svg?branch=master)](https://travis-ci.org/yohanboniface/falcon-multipart)

# Parse multipart/form-data requests in Falcon

## Install

    pip install falcon-multipart


## Usage

Add the `MultipartMiddleware` to your api middlewares:

    from falcon_multipart.middleware import MultipartMiddleware

    api = falcon.API(middleware=[MultipartMiddleware()])

This will parse any `multipart/form-data` incoming request, and put the keys
in `req._params`, including files, so you get the field as other params.


## Dealing with files

Files will be available as [`cgi.FieldStorage`](https://docs.python.org/3/library/cgi.html),
with following main parameters:

- `file`: act as a python file, you can call `read()` on it, and you will
  retrieve content (as *bytes*)
- `filename`: the filename, if given
- `value`: the file content in *bytes*
- `type`: the content-type, or None if not specified
- `disposition`: content-disposition, or None if not specified


## Example

    # Say you have a form with those fields:
    # - title => a string
    # - image => an image file

    def on_post(req, resp, **kwargs):
        title = req.get_param('title')
        image = req.get_param('image')
        # Read image as binary
        raw = image.file.read()
        # Retrieve filename
        filename = image.filename

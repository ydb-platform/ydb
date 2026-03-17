# pillow-heif

[![Analysis & Coverage](https://github.com/bigcat88/pillow_heif/actions/workflows/analysis-coverage.yml/badge.svg)](https://github.com/bigcat88/pillow_heif/actions/workflows/analysis-coverage.yml)
[![Nightly build](https://github.com/bigcat88/pillow_heif/actions/workflows/nightly-src-build.yml/badge.svg)](https://github.com/bigcat88/pillow_heif/actions/workflows/nightly-src-build.yml)
[![docs](https://readthedocs.org/projects/pillow-heif/badge/?version=latest)](https://pillow-heif.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/bigcat88/pillow_heif/branch/master/graph/badge.svg?token=JY64F2OL6V)](https://codecov.io/gh/bigcat88/pillow_heif)

![PythonVersion](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)
![impl](https://img.shields.io/pypi/implementation/pillow_heif)
![pypi](https://img.shields.io/pypi/v/pillow_heif.svg)
[![Downloads](https://static.pepy.tech/personalized-badge/pillow-heif?period=total&units=international_system&left_color=grey&right_color=orange&left_text=Downloads)](https://pepy.tech/project/pillow-heif)
[![Downloads](https://static.pepy.tech/personalized-badge/pillow-heif?period=month&units=international_system&left_color=grey&right_color=orange&left_text=Downloads/Month)](https://pepy.tech/project/pillow-heif)

![Mac OS](https://img.shields.io/badge/mac%20os-FCC624?style=for-the-badge&logoColor=white)
![Windows](https://img.shields.io/badge/Windows-0078D6?style=for-the-badge&logo=windows&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=for-the-badge&logo=linux&logoColor=black)
![Alpine Linux](https://img.shields.io/badge/Alpine_Linux-0078D6.svg?style=for-the-badge&logo=alpine-linux&logoColor=white)
![Raspberry Pi](https://img.shields.io/badge/Rasberry_Pi-FCC624.svg?style=for-the-badge&logo=raspberry-pi&logoColor=red)

Python bindings to [libheif](https://github.com/strukturag/libheif) for working with HEIF images and plugin for Pillow.

Features:
 * Decoding of `8`, `10`, `12` bit HEIC files.
 * Encoding of `8`, `10`, `12` bit HEIC files.
 * `EXIF`, `XMP`, `IPTC` read & write support.
 * Support of multiple images in one file and a `PrimaryImage` attribute.
 * Adding & removing `thumbnails`.
 * Reading of `Depth Images`.
 * (beta) Reading of `Auxiliary Images` by [johncf](https://github.com/johncf)
 * Adding HEIF support to Pillow in one line of code as a plugin.

Note: Here is a light version [pi-heif](https://pypi.org/project/pi-heif/) of this project without encoding capabilities.

### Install
```console
python3 -m pip install -U pip
python3 -m pip install pillow-heif
```

### Example of use as a Pillow plugin
```python3
from PIL import Image
from pillow_heif import register_heif_opener

register_heif_opener()

im = Image.open("image.heic")  # do whatever need with a Pillow image
im = im.rotate(13)
im.save(f"rotated_image.heic", quality=90)
```

### 16 bit PNG to 10 bit HEIF using OpenCV
```python3
import cv2
import pillow_heif

cv_img = cv2.imread("16bit_with_alpha.png", cv2.IMREAD_UNCHANGED)
heif_file = pillow_heif.from_bytes(
    mode="BGRA;16",
    size=(cv_img.shape[1], cv_img.shape[0]),
    data=bytes(cv_img)
)
heif_file.save("RGBA_10bit.heic", quality=-1)
```

### 8/10/12 bit HEIF to 8/16 bit PNG using OpenCV
```python3
import numpy as np
import cv2
import pillow_heif

heif_file = pillow_heif.open_heif("image.heic", convert_hdr_to_8bit=False, bgr_mode=True)
np_array = np.asarray(heif_file)
cv2.imwrite("image.png", np_array)
```

### Accessing decoded image data
```python3
import pillow_heif

if pillow_heif.is_supported("image.heic"):
    heif_file = pillow_heif.open_heif("image.heic", convert_hdr_to_8bit=False)
    print("image size:", heif_file.size)
    print("image mode:", heif_file.mode)
    print("image data length:", len(heif_file.data))
    print("image data stride:", heif_file.stride)
```

### Get decoded image data as a Numpy array
```python3
import numpy as np
import pillow_heif

if pillow_heif.is_supported("input.heic"):
    heif_file = pillow_heif.open_heif("input.heic")
    np_array = np.asarray(heif_file)
```

### Accessing Depth Images

```python3
from PIL import Image
from pillow_heif import register_heif_opener
import numpy as np

register_heif_opener()

im = Image.open("../tests/images/heif_other/pug.heic")
if im.info["depth_images"]:
    depth_im = im.info["depth_images"][0]  # Access the first depth image (usually there will be only one).
    # Depth images are instances of `class HeifDepthImage(BaseImage)`,
    # so work with them as you would with any usual image in pillow_heif.
    # Depending on what you need the depth image for, you can convert it to a NumPy array or convert it to a Pillow image.
    pil_im = depth_im.to_pillow()
    np_im = np.asarray(depth_im)
    print(pil_im)
    print(pil_im.info["metadata"])
```


### More Information

- [Documentation](https://pillow-heif.readthedocs.io/)
  - [Installation](https://pillow-heif.readthedocs.io/en/latest/installation.html)
  - [Pillow plugin](https://pillow-heif.readthedocs.io/en/latest/pillow-plugin.html)
  - [Using HeifFile](https://pillow-heif.readthedocs.io/en/latest/heif-file.html)
  - [Image modes](https://pillow-heif.readthedocs.io/en/latest/image-modes.html)
  - [Options](https://pillow-heif.readthedocs.io/en/latest/options.html)
- [Examples](https://github.com/bigcat88/pillow_heif/tree/master/examples)
- [Contribute](https://github.com/bigcat88/pillow_heif/blob/master/.github/CONTRIBUTING.md)
  - [Discussions](https://github.com/bigcat88/pillow_heif/discussions)
  - [Issues](https://github.com/bigcat88/pillow_heif/issues)
- [Changelog](https://github.com/bigcat88/pillow_heif/blob/master/CHANGELOG.md)

### Wheels

| **_Wheels table_** | macOS<br/>Intel | macOS<br/>Silicon | Windows<br/> | musllinux* | manylinux* |
|--------------------|:---------------:|:-----------------:|:------------:|:----------:|:----------:|
| CPython 3.10       |        ✅        |         ✅         |      ✅       |     ✅      |     ✅      |
| CPython 3.11       |        ✅        |         ✅         |      ✅       |     ✅      |     ✅      |
| CPython 3.12       |        ✅        |         ✅         |      ✅       |     ✅      |     ✅      |
| CPython 3.13       |        ✅        |         ✅         |      ✅       |     ✅      |     ✅      |
| CPython 3.14       |        ✅        |         ✅         |      ✅       |     ✅      |     ✅      |
| CPython 3.14t      |        ✅        |         ✅         |      ✅       |     ✅      |     ✅      |
| PyPy 3.11 v7.3     |        ✅        |         ✅         |      ✅       |    N/A     |     ✅      |

&ast; **x86_64**, **aarch64** wheels.

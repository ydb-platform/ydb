# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

"""
specs module contains classes that help you define and create specs for use
in the Ads API.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from facebook_business.adobjects.abstractobject  import AbstractObject
from facebook_business.mixins import ValidatesFields
from facebook_business.adobjects import adcreativeobjectstoryspec
from facebook_business.adobjects import adcreativelinkdatachildattachment
from facebook_business.adobjects import adcreativelinkdata
from facebook_business.adobjects import adcreativetextdata
from facebook_business.adobjects import adcreativephotodata
from facebook_business.adobjects import adcreativevideodata
from facebook_business.adobjects import pagepost
from facebook_business.adobjects import user


class ObjectStorySpec(adcreativeobjectstoryspec.AdCreativeObjectStorySpec):
    pass


class AttachmentData(adcreativelinkdatachildattachment.AdCreativeLinkDataChildAttachment):
    pass


class LinkData(adcreativelinkdata.AdCreativeLinkData):
    pass


class TemplateData(adcreativelinkdata.AdCreativeLinkData):
    pass


class TextData(adcreativetextdata.AdCreativeTextData):
    pass


class PhotoData(adcreativephotodata.AdCreativePhotoData):
    pass


class VideoData(adcreativevideodata.AdCreativeVideoData):
    pass

class PagePostData(pagepost.PagePost):
    pass

class UserData(user.User):
    pass

class SlideshowSpec(ValidatesFields, AbstractObject):
    class Field(object):
        images_urls = 'images_urls'
        duration_ms = 'duration_ms'
        transition_ms = 'transition_ms'

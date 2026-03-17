# Copyright 2014 Facebook, Inc.

# You are hereby granted a non-exclusive, worldwide, royalty-free license to
# use, copy, modify, and distribute this software in source code or binary
# form for use in connection with the web services and APIs provided by
# Facebook.

# As with any software that integrates with the Facebook platform, your use
# of this software is subject to the Facebook Developer Principles and
# Policies [http://developers.facebook.com/policy/]. This copyright notice
# shall be included in all copies or substantial portions of the software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from enum import Enum


# Used to specify where the conversion occurred.
# See https://developers.facebook.com/docs/marketing-api/conversions-api/parameters/server-event#action-source
class ActionSource(Enum):

    """
    Conversion happened over email.
    """
    EMAIL = 'email'

    """
    Conversion was made on your website.
    """
    WEBSITE = 'website'

    """
    Conversion was made using your app.
    """
    APP = 'app'

    """
    Conversion was made over the phone.
    """
    PHONE_CALL = 'phone_call'

    """
    Conversion was made via a messaging app, SMS, or online messaging feature.
    """
    CHAT = 'chat'

    """
    Conversion was made in person at your physical store.
    """
    PHYSICAL_STORE = 'physical_store'

    """
    Conversion happened automatically, for example, a subscription renewal that's set on auto-pay each month.
    """
    SYSTEM_GENERATED = 'system_generated'

    """
    Conversion happened through a business messaging channel, such as WhatsApp or Instagram Direct.
    """
    BUSINESS_MESSAGING = 'business_messaging'

    """
    Conversion happened in a way that is not listed.
    """
    OTHER = 'other'

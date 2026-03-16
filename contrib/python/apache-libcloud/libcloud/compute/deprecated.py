# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Database of deprecated drivers
"""

from libcloud.compute.types import Provider

DEPRECATED_DRIVERS = {
    Provider.OPSOURCE: {
        "reason": "OpSource cloud is now part of Dimension Data, "
        "use the DIMENSIONDATA provider instead.",
        "url": "http://www.ntt.co.jp/news2011/1107e/110701a.html",
    },
    Provider.NINEFOLD: {
        "reason": "We will shortly notify our customers that we "
        "will be sunsetting our Public Cloud Computing "
        "(Server) platform, the last day of operation "
        "being January 30, 2016",
        "url": "https://ninefold.com/news/",
    },
    Provider.IBM: {
        "reason": "IBM SmartCloud Enterprise has been deprecated "
        "in favour of IBM SoftLayer Public Cloud, please"
        " use the SOFTLAYER provider.",
        "url": "http://www.ibm.com/midmarket/us/en/article_cloud6_1310.html",
    },
    Provider.HPCLOUD: {
        "reason": "HP Helion Public Cloud was shut down in January 2016.",
        "url": "http://libcloud.apache.org/blog/" "2016/02/16/new-drivers-deprecated-drivers.html",
    },
    Provider.CLOUDFRAMES: {
        "reason": "The CloudFrames Provider is no longer supported",
        "url": "http://libcloud.apache.org/blog/2016/02/16/new-drivers-" "deprecated-drivers.html",
    },
    Provider.RUNABOVE: {
        "reason": "The RunAbove compute is no longer supported. " "Use the OVH one instead.",
        "url": "https://www.runabove.com/cloud-instance.xml",
    },
}

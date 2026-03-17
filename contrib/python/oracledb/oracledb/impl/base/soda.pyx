#------------------------------------------------------------------------------
# Copyright (c) 2020, 2024, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# soda.pyx
#
# Cython file defining the base SODA implementation classes (embedded in
# base_impl.pyx).
#------------------------------------------------------------------------------

cdef class BaseSodaDbImpl:

    def create_collection(self, str name, str metadata, bint map_mode):
        errors._raise_not_supported("creating a SODA collection")

    def create_document(self, bytes content, str key, str media_type):
        errors._raise_not_supported("creating a SODA binary/text document")

    def create_json_document(self, object content, str key):
        errors._raise_not_supported("creating a SODA JSON document")

    def get_collection_names(self, str start_name, uint32_t limit):
        errors._raise_not_supported("getting a list of SODA collection names")

    def open_collection(self, str name):
        errors._raise_not_supported("opening a SODA collection")


cdef class BaseSodaCollImpl:

    def create_index(self, str spec):
        errors._raise_not_supported("creating an index on a SODA collection")

    def drop(self):
        errors._raise_not_supported("dropping a SODA collection")

    def drop_index(self, str name, bint force):
        errors._raise_not_supported("dropping an index on a SODA collection")

    def get_count(self, object op):
        errors._raise_not_supported(
            "getting the count of documents in a SODA collection"
        )

    def get_cursor(self, object op):
        errors._raise_not_supported(
            "getting a cursor for documents in a SODA collection"
        )

    def get_data_guide(self):
        errors._raise_not_supported(
            "getting the data guide for a SODA collection"
        )

    def get_metadata(self):
        errors._raise_not_supported(
            "getting the metadata of a SODA collection"
        )

    def get_one(self, object op):
        errors._raise_not_supported(
            "getting a document from a SODA collection"
        )

    def insert_many(self, list documents, str hint, bint return_docs):
        errors._raise_not_supported(
            "inserting multiple documents into a SODA collection"
        )

    def insert_one(self, BaseSodaDocImpl doc, str hint, bint return_doc):
        errors._raise_not_supported(
            "inserting a single document into a SODA collection"
        )

    def remove(self, object op):
        errors._raise_not_supported(
            "removing documents from a SODA collection"
        )

    def replace_one(self, BaseSodaDocImpl doc_impl, bint return_doc):
        errors._raise_not_supported(
            "replacing a document in a SODA collection"
        )

    def save(self, BaseSodaDocImpl doc, str hint, bint return_doc):
        errors._raise_not_supported("saving a document in a SODA collection")

    def truncate(self):
        errors._raise_not_supported("truncating a SODA collection")


cdef class BaseSodaDocImpl:

    def get_content(self):
        errors._raise_not_supported("getting the content of a SODA document")

    def get_created_on(self):
        errors._raise_not_supported(
            "getting the created on date of a SODA document"
        )

    def get_key(self):
        errors._raise_not_supported("getting the key of a SODA document")

    def get_last_modified(self):
        errors._raise_not_supported(
            "getting the last modified date of a SODA document"
        )

    def get_media_type(self):
        errors._raise_not_supported(
            "getting the media type of a SODA document"
        )

    def get_version(self):
        errors._raise_not_supported("getting the version of a SODA document")


cdef class BaseSodaDocCursorImpl:

    def close(self):
        errors._raise_not_supported("closing a SODA document cursor")

    def get_next_doc(self):
        errors._raise_not_supported(
            "getting the next document from a SODA document cursor"
        )

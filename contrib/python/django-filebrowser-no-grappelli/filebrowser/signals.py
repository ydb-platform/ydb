# coding: utf-8

from django.dispatch import Signal


# upload signals
# path: Absolute server path to the file/folder
# name: Name of the file/folder
# site: Current FileBrowserSite instance
filebrowser_pre_upload = Signal()  # providing_args=["path", "file", "site"])
filebrowser_post_upload = Signal()  # providing_args=["path", "file", "site"])

# mkdir signals
# path: Absolute server path to the file/folder
# name: Name of the file/folder
# site: Current FileBrowserSite instance
filebrowser_pre_createdir = Signal()  # providing_args=["path", "name", "site"])
filebrowser_post_createdir = Signal()  # providing_args=["path", "name", "site"])

# delete signals
# path: Absolute server path to the file/folder
# name: Name of the file/folder
# site: Current FileBrowserSite instance
filebrowser_pre_delete = Signal()  # providing_args=["path", "name", "site"])
filebrowser_post_delete = Signal()  # providing_args=["path", "name", "site"])

# rename signals
# path: Absolute server path to the file/folder
# name: Name of the file/folder
# site: Current FileBrowserSite instance
# new_name: New name of the file/folder
filebrowser_pre_rename = Signal()  # providing_args=["path", "name", "new_name", "site"])
filebrowser_post_rename = Signal()  # providing_args=["path", "name", "new_name", "site"])

# action signals
# action_name: Name of the custom action
# fileobjects: A list of fileobjects the action will be applied to
# site: Current FileBrowserSite instance
# result: The response you defined with your custom action
filebrowser_actions_pre_apply = Signal()  # providing_args=['action_name', 'fileobjects', 'site'])
filebrowser_actions_post_apply = Signal()  # providing_args=['action_name', 'filebjects', 'result', 'site'])

XZ
===
`TXzDecompress` supports file formats:
1) `.xz` - could be generated with CLI-tool `xz`. This format allows concatenating compressed files as is:
```
    echo foo | xz > foobar.xz
    echo bar | xz >> foobar.xz
```
2) `.lzma` - could be generated with CLI-tool `lzma` - it is legacy: https://fossies.org/linux/xz/README

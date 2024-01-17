// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package os

import (
	"internal/syscall/windows"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// A fileStat is the implementation of FileInfo returned by Stat and Lstat.
type fileStat struct {
	name string

	// from ByHandleFileInformation, Win32FileAttributeData and Win32finddata
	FileAttributes uint32
	CreationTime   syscall.Filetime
	LastAccessTime syscall.Filetime
	LastWriteTime  syscall.Filetime
	FileSizeHigh   uint32
	FileSizeLow    uint32

	// from Win32finddata
	ReparseTag uint32

	// what syscall.GetFileType returns
	filetype uint32

	// used to implement SameFile
	sync.Mutex
	path             string
	vol              uint32
	idxhi            uint32
	idxlo            uint32
	appendNameToPath bool
}

// newFileStatFromGetFileInformationByHandle calls GetFileInformationByHandle
// to gather all required information about the file handle h.
func newFileStatFromGetFileInformationByHandle(path string, h syscall.Handle) (fs *fileStat, err error) {
	var d syscall.ByHandleFileInformation
	err = syscall.GetFileInformationByHandle(h, &d)
	if err != nil {
		return nil, &PathError{Op: "GetFileInformationByHandle", Path: path, Err: err}
	}

	var ti windows.FILE_ATTRIBUTE_TAG_INFO
	err = windows.GetFileInformationByHandleEx(h, windows.FileAttributeTagInfo, (*byte)(unsafe.Pointer(&ti)), uint32(unsafe.Sizeof(ti)))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok && errno == windows.ERROR_INVALID_PARAMETER {
			// It appears calling GetFileInformationByHandleEx with
			// FILE_ATTRIBUTE_TAG_INFO fails on FAT file system with
			// ERROR_INVALID_PARAMETER. Clear ti.ReparseTag in that
			// instance to indicate no symlinks are possible.
			ti.ReparseTag = 0
		} else {
			return nil, &PathError{Op: "GetFileInformationByHandleEx", Path: path, Err: err}
		}
	}

	return &fileStat{
		name:           basename(path),
		FileAttributes: d.FileAttributes,
		CreationTime:   d.CreationTime,
		LastAccessTime: d.LastAccessTime,
		LastWriteTime:  d.LastWriteTime,
		FileSizeHigh:   d.FileSizeHigh,
		FileSizeLow:    d.FileSizeLow,
		vol:            d.VolumeSerialNumber,
		idxhi:          d.FileIndexHigh,
		idxlo:          d.FileIndexLow,
		ReparseTag:     ti.ReparseTag,
		// fileStat.path is used by os.SameFile to decide if it needs
		// to fetch vol, idxhi and idxlo. But these are already set,
		// so set fileStat.path to "" to prevent os.SameFile doing it again.
	}, nil
}

// newFileStatFromWin32finddata copies all required information
// from syscall.Win32finddata d into the newly created fileStat.
func newFileStatFromWin32finddata(d *syscall.Win32finddata) *fileStat {
	fs := &fileStat{
		FileAttributes: d.FileAttributes,
		CreationTime:   d.CreationTime,
		LastAccessTime: d.LastAccessTime,
		LastWriteTime:  d.LastWriteTime,
		FileSizeHigh:   d.FileSizeHigh,
		FileSizeLow:    d.FileSizeLow,
	}
	if d.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		// Per https://learn.microsoft.com/en-us/windows/win32/api/minwinbase/ns-minwinbase-win32_find_dataw:
		// “If the dwFileAttributes member includes the FILE_ATTRIBUTE_REPARSE_POINT
		// attribute, this member specifies the reparse point tag. Otherwise, this
		// value is undefined and should not be used.”
		fs.ReparseTag = d.Reserved0
	}
	return fs
}

func (fs *fileStat) isSymlink() bool {
	// As of https://go.dev/cl/86556, we treat MOUNT_POINT reparse points as
	// symlinks because otherwise certain directory junction tests in the
	// path/filepath package would fail.
	//
	// However,
	// https://learn.microsoft.com/en-us/windows/win32/fileio/hard-links-and-junctions
	// seems to suggest that directory junctions should be treated like hard
	// links, not symlinks.
	//
	// TODO(bcmills): Get more input from Microsoft on what the behavior ought to
	// be for MOUNT_POINT reparse points.

	return fs.ReparseTag == syscall.IO_REPARSE_TAG_SYMLINK ||
		fs.ReparseTag == windows.IO_REPARSE_TAG_MOUNT_POINT
}

func (fs *fileStat) Size() int64 {
	return int64(fs.FileSizeHigh)<<32 + int64(fs.FileSizeLow)
}

func (fs *fileStat) Mode() (m FileMode) {
	if fs.FileAttributes&syscall.FILE_ATTRIBUTE_READONLY != 0 {
		m |= 0444
	} else {
		m |= 0666
	}
	if fs.isSymlink() {
		return m | ModeSymlink
	}
	if fs.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 {
		m |= ModeDir | 0111
	}
	switch fs.filetype {
	case syscall.FILE_TYPE_PIPE:
		m |= ModeNamedPipe
	case syscall.FILE_TYPE_CHAR:
		m |= ModeDevice | ModeCharDevice
	}
	if fs.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0 && m&ModeType == 0 {
		m |= ModeIrregular
	}
	return m
}

func (fs *fileStat) ModTime() time.Time {
	return time.Unix(0, fs.LastWriteTime.Nanoseconds())
}

// Sys returns syscall.Win32FileAttributeData for file fs.
func (fs *fileStat) Sys() any {
	return &syscall.Win32FileAttributeData{
		FileAttributes: fs.FileAttributes,
		CreationTime:   fs.CreationTime,
		LastAccessTime: fs.LastAccessTime,
		LastWriteTime:  fs.LastWriteTime,
		FileSizeHigh:   fs.FileSizeHigh,
		FileSizeLow:    fs.FileSizeLow,
	}
}

func (fs *fileStat) loadFileId() error {
	fs.Lock()
	defer fs.Unlock()
	if fs.path == "" {
		// already done
		return nil
	}
	var path string
	if fs.appendNameToPath {
		path = fs.path + `\` + fs.name
	} else {
		path = fs.path
	}
	pathp, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return err
	}

	// Per https://learn.microsoft.com/en-us/windows/win32/fileio/reparse-points-and-file-operations,
	// “Applications that use the CreateFile function should specify the
	// FILE_FLAG_OPEN_REPARSE_POINT flag when opening the file if it is a reparse
	// point.”
	//
	// And per https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew,
	// “If the file is not a reparse point, then this flag is ignored.”
	//
	// So we set FILE_FLAG_OPEN_REPARSE_POINT unconditionally, since we want
	// information about the reparse point itself.
	//
	// If the file is a symlink, the symlink target should have already been
	// resolved when the fileStat was created, so we don't need to worry about
	// resolving symlink reparse points again here.
	attrs := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS | syscall.FILE_FLAG_OPEN_REPARSE_POINT)

	h, err := syscall.CreateFile(pathp, 0, 0, nil, syscall.OPEN_EXISTING, attrs, 0)
	if err != nil {
		return err
	}
	defer syscall.CloseHandle(h)
	var i syscall.ByHandleFileInformation
	err = syscall.GetFileInformationByHandle(h, &i)
	if err != nil {
		return err
	}
	fs.path = ""
	fs.vol = i.VolumeSerialNumber
	fs.idxhi = i.FileIndexHigh
	fs.idxlo = i.FileIndexLow
	return nil
}

// saveInfoFromPath saves full path of the file to be used by os.SameFile later,
// and set name from path.
func (fs *fileStat) saveInfoFromPath(path string) error {
	fs.path = path
	if !isAbs(fs.path) {
		var err error
		fs.path, err = syscall.FullPath(fs.path)
		if err != nil {
			return &PathError{Op: "FullPath", Path: path, Err: err}
		}
	}
	fs.name = basename(path)
	return nil
}

func sameFile(fs1, fs2 *fileStat) bool {
	e := fs1.loadFileId()
	if e != nil {
		return false
	}
	e = fs2.loadFileId()
	if e != nil {
		return false
	}
	return fs1.vol == fs2.vol && fs1.idxhi == fs2.idxhi && fs1.idxlo == fs2.idxlo
}

// For testing.
func atime(fi FileInfo) time.Time {
	return time.Unix(0, fi.Sys().(*syscall.Win32FileAttributeData).LastAccessTime.Nanoseconds())
}

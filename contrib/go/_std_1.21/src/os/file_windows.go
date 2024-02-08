// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package os

import (
	"errors"
	"internal/poll"
	"internal/syscall/windows"
	"runtime"
	"sync"
	"syscall"
	"unsafe"
)

// This matches the value in syscall/syscall_windows.go.
const _UTIME_OMIT = -1

// file is the real representation of *File.
// The extra level of indirection ensures that no clients of os
// can overwrite this data, which could cause the finalizer
// to close the wrong file descriptor.
type file struct {
	pfd        poll.FD
	name       string
	dirinfo    *dirInfo // nil unless directory being read
	appendMode bool     // whether file is opened for appending
}

// Fd returns the Windows handle referencing the open file.
// If f is closed, the file descriptor becomes invalid.
// If f is garbage collected, a finalizer may close the file descriptor,
// making it invalid; see runtime.SetFinalizer for more information on when
// a finalizer might be run. On Unix systems this will cause the SetDeadline
// methods to stop working.
func (file *File) Fd() uintptr {
	if file == nil {
		return uintptr(syscall.InvalidHandle)
	}
	return uintptr(file.pfd.Sysfd)
}

// newFile returns a new File with the given file handle and name.
// Unlike NewFile, it does not check that h is syscall.InvalidHandle.
func newFile(h syscall.Handle, name string, kind string) *File {
	if kind == "file" {
		var m uint32
		if syscall.GetConsoleMode(h, &m) == nil {
			kind = "console"
		}
		if t, err := syscall.GetFileType(h); err == nil && t == syscall.FILE_TYPE_PIPE {
			kind = "pipe"
		}
	}

	f := &File{&file{
		pfd: poll.FD{
			Sysfd:         h,
			IsStream:      true,
			ZeroReadIsEOF: true,
		},
		name: name,
	}}
	runtime.SetFinalizer(f.file, (*file).close)

	// Ignore initialization errors.
	// Assume any problems will show up in later I/O.
	f.pfd.Init(kind, false)

	return f
}

// newConsoleFile creates new File that will be used as console.
func newConsoleFile(h syscall.Handle, name string) *File {
	return newFile(h, name, "console")
}

// NewFile returns a new File with the given file descriptor and
// name. The returned value will be nil if fd is not a valid file
// descriptor.
func NewFile(fd uintptr, name string) *File {
	h := syscall.Handle(fd)
	if h == syscall.InvalidHandle {
		return nil
	}
	return newFile(h, name, "file")
}

// Auxiliary information if the File describes a directory
type dirInfo struct {
	h       syscall.Handle // search handle created with FindFirstFile
	data    syscall.Win32finddata
	path    string
	isempty bool // set if FindFirstFile returns ERROR_FILE_NOT_FOUND
}

func (d *dirInfo) close() error {
	return syscall.FindClose(d.h)
}

func epipecheck(file *File, e error) {
}

// DevNull is the name of the operating system's “null device.”
// On Unix-like systems, it is "/dev/null"; on Windows, "NUL".
const DevNull = "NUL"

func openDir(name string) (d *dirInfo, e error) {
	var mask string

	path := fixLongPath(name)

	if len(path) == 2 && path[1] == ':' { // it is a drive letter, like C:
		mask = path + `*`
	} else if len(path) > 0 {
		lc := path[len(path)-1]
		if lc == '/' || lc == '\\' {
			mask = path + `*`
		} else {
			mask = path + `\*`
		}
	} else {
		mask = `\*`
	}
	maskp, e := syscall.UTF16PtrFromString(mask)
	if e != nil {
		return nil, e
	}
	d = new(dirInfo)
	d.h, e = syscall.FindFirstFile(maskp, &d.data)
	if e != nil {
		// FindFirstFile returns ERROR_FILE_NOT_FOUND when
		// no matching files can be found. Then, if directory
		// exists, we should proceed.
		// If FindFirstFile failed because name does not point
		// to a directory, we should return ENOTDIR.
		var fa syscall.Win32FileAttributeData
		pathp, e1 := syscall.UTF16PtrFromString(path)
		if e1 != nil {
			return nil, e
		}
		e1 = syscall.GetFileAttributesEx(pathp, syscall.GetFileExInfoStandard, (*byte)(unsafe.Pointer(&fa)))
		if e1 != nil {
			return nil, e
		}
		if fa.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY == 0 {
			return nil, syscall.ENOTDIR
		}
		if e != syscall.ERROR_FILE_NOT_FOUND {
			return nil, e
		}
		d.isempty = true
	}
	d.path = path
	if !isAbs(d.path) {
		d.path, e = syscall.FullPath(d.path)
		if e != nil {
			d.close()
			return nil, e
		}
	}
	return d, nil
}

// openFileNolog is the Windows implementation of OpenFile.
func openFileNolog(name string, flag int, perm FileMode) (*File, error) {
	if name == "" {
		return nil, &PathError{Op: "open", Path: name, Err: syscall.ENOENT}
	}
	path := fixLongPath(name)
	r, e := syscall.Open(path, flag|syscall.O_CLOEXEC, syscallMode(perm))
	if e != nil {
		// We should return EISDIR when we are trying to open a directory with write access.
		if e == syscall.ERROR_ACCESS_DENIED && (flag&O_WRONLY != 0 || flag&O_RDWR != 0) {
			pathp, e1 := syscall.UTF16PtrFromString(path)
			if e1 == nil {
				var fa syscall.Win32FileAttributeData
				e1 = syscall.GetFileAttributesEx(pathp, syscall.GetFileExInfoStandard, (*byte)(unsafe.Pointer(&fa)))
				if e1 == nil && fa.FileAttributes&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 {
					e = syscall.EISDIR
				}
			}
		}
		return nil, &PathError{Op: "open", Path: name, Err: e}
	}
	f, e := newFile(r, name, "file"), nil
	if e != nil {
		return nil, &PathError{Op: "open", Path: name, Err: e}
	}
	return f, nil
}

func (file *file) close() error {
	if file == nil {
		return syscall.EINVAL
	}
	if file.dirinfo != nil {
		file.dirinfo.close()
		file.dirinfo = nil
	}
	var err error
	if e := file.pfd.Close(); e != nil {
		if e == poll.ErrFileClosing {
			e = ErrClosed
		}
		err = &PathError{Op: "close", Path: file.name, Err: e}
	}

	// no need for a finalizer anymore
	runtime.SetFinalizer(file, nil)
	return err
}

// seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
func (f *File) seek(offset int64, whence int) (ret int64, err error) {
	if f.dirinfo != nil {
		// Free cached dirinfo, so we allocate a new one if we
		// access this file as a directory again. See #35767 and #37161.
		f.dirinfo.close()
		f.dirinfo = nil
	}
	ret, err = f.pfd.Seek(offset, whence)
	runtime.KeepAlive(f)
	return ret, err
}

// Truncate changes the size of the named file.
// If the file is a symbolic link, it changes the size of the link's target.
func Truncate(name string, size int64) error {
	f, e := OpenFile(name, O_WRONLY, 0666)
	if e != nil {
		return e
	}
	defer f.Close()
	e1 := f.Truncate(size)
	if e1 != nil {
		return e1
	}
	return nil
}

// Remove removes the named file or directory.
// If there is an error, it will be of type *PathError.
func Remove(name string) error {
	p, e := syscall.UTF16PtrFromString(fixLongPath(name))
	if e != nil {
		return &PathError{Op: "remove", Path: name, Err: e}
	}

	// Go file interface forces us to know whether
	// name is a file or directory. Try both.
	e = syscall.DeleteFile(p)
	if e == nil {
		return nil
	}
	e1 := syscall.RemoveDirectory(p)
	if e1 == nil {
		return nil
	}

	// Both failed: figure out which error to return.
	if e1 != e {
		a, e2 := syscall.GetFileAttributes(p)
		if e2 != nil {
			e = e2
		} else {
			if a&syscall.FILE_ATTRIBUTE_DIRECTORY != 0 {
				e = e1
			} else if a&syscall.FILE_ATTRIBUTE_READONLY != 0 {
				if e1 = syscall.SetFileAttributes(p, a&^syscall.FILE_ATTRIBUTE_READONLY); e1 == nil {
					if e = syscall.DeleteFile(p); e == nil {
						return nil
					}
				}
			}
		}
	}
	return &PathError{Op: "remove", Path: name, Err: e}
}

func rename(oldname, newname string) error {
	e := windows.Rename(fixLongPath(oldname), fixLongPath(newname))
	if e != nil {
		return &LinkError{"rename", oldname, newname, e}
	}
	return nil
}

// Pipe returns a connected pair of Files; reads from r return bytes written to w.
// It returns the files and an error, if any. The Windows handles underlying
// the returned files are marked as inheritable by child processes.
func Pipe() (r *File, w *File, err error) {
	var p [2]syscall.Handle
	e := syscall.Pipe(p[:])
	if e != nil {
		return nil, nil, NewSyscallError("pipe", e)
	}
	return newFile(p[0], "|0", "pipe"), newFile(p[1], "|1", "pipe"), nil
}

var (
	useGetTempPath2Once sync.Once
	useGetTempPath2     bool
)

func tempDir() string {
	useGetTempPath2Once.Do(func() {
		useGetTempPath2 = (windows.ErrorLoadingGetTempPath2() == nil)
	})
	getTempPath := syscall.GetTempPath
	if useGetTempPath2 {
		getTempPath = windows.GetTempPath2
	}
	n := uint32(syscall.MAX_PATH)
	for {
		b := make([]uint16, n)
		n, _ = getTempPath(uint32(len(b)), &b[0])
		if n > uint32(len(b)) {
			continue
		}
		if n == 3 && b[1] == ':' && b[2] == '\\' {
			// Do nothing for path, like C:\.
		} else if n > 0 && b[n-1] == '\\' {
			// Otherwise remove terminating \.
			n--
		}
		return syscall.UTF16ToString(b[:n])
	}
}

// Link creates newname as a hard link to the oldname file.
// If there is an error, it will be of type *LinkError.
func Link(oldname, newname string) error {
	n, err := syscall.UTF16PtrFromString(fixLongPath(newname))
	if err != nil {
		return &LinkError{"link", oldname, newname, err}
	}
	o, err := syscall.UTF16PtrFromString(fixLongPath(oldname))
	if err != nil {
		return &LinkError{"link", oldname, newname, err}
	}
	err = syscall.CreateHardLink(n, o, 0)
	if err != nil {
		return &LinkError{"link", oldname, newname, err}
	}
	return nil
}

// Symlink creates newname as a symbolic link to oldname.
// On Windows, a symlink to a non-existent oldname creates a file symlink;
// if oldname is later created as a directory the symlink will not work.
// If there is an error, it will be of type *LinkError.
func Symlink(oldname, newname string) error {
	// '/' does not work in link's content
	oldname = fromSlash(oldname)

	// need the exact location of the oldname when it's relative to determine if it's a directory
	destpath := oldname
	if v := volumeName(oldname); v == "" {
		if len(oldname) > 0 && IsPathSeparator(oldname[0]) {
			// oldname is relative to the volume containing newname.
			if v = volumeName(newname); v != "" {
				// Prepend the volume explicitly, because it may be different from the
				// volume of the current working directory.
				destpath = v + oldname
			}
		} else {
			// oldname is relative to newname.
			destpath = dirname(newname) + `\` + oldname
		}
	}

	fi, err := Stat(destpath)
	isdir := err == nil && fi.IsDir()

	n, err := syscall.UTF16PtrFromString(fixLongPath(newname))
	if err != nil {
		return &LinkError{"symlink", oldname, newname, err}
	}
	o, err := syscall.UTF16PtrFromString(fixLongPath(oldname))
	if err != nil {
		return &LinkError{"symlink", oldname, newname, err}
	}

	var flags uint32 = windows.SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE
	if isdir {
		flags |= syscall.SYMBOLIC_LINK_FLAG_DIRECTORY
	}
	err = syscall.CreateSymbolicLink(n, o, flags)
	if err != nil {
		// the unprivileged create flag is unsupported
		// below Windows 10 (1703, v10.0.14972). retry without it.
		flags &^= windows.SYMBOLIC_LINK_FLAG_ALLOW_UNPRIVILEGED_CREATE
		err = syscall.CreateSymbolicLink(n, o, flags)
		if err != nil {
			return &LinkError{"symlink", oldname, newname, err}
		}
	}
	return nil
}

// openSymlink calls CreateFile Windows API with FILE_FLAG_OPEN_REPARSE_POINT
// parameter, so that Windows does not follow symlink, if path is a symlink.
// openSymlink returns opened file handle.
func openSymlink(path string) (syscall.Handle, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	attrs := uint32(syscall.FILE_FLAG_BACKUP_SEMANTICS)
	// Use FILE_FLAG_OPEN_REPARSE_POINT, otherwise CreateFile will follow symlink.
	// See https://docs.microsoft.com/en-us/windows/desktop/FileIO/symbolic-link-effects-on-file-systems-functions#createfile-and-createfiletransacted
	attrs |= syscall.FILE_FLAG_OPEN_REPARSE_POINT
	h, err := syscall.CreateFile(p, 0, 0, nil, syscall.OPEN_EXISTING, attrs, 0)
	if err != nil {
		return 0, err
	}
	return h, nil
}

// normaliseLinkPath converts absolute paths returned by
// DeviceIoControl(h, FSCTL_GET_REPARSE_POINT, ...)
// into paths acceptable by all Windows APIs.
// For example, it converts
//
//	\??\C:\foo\bar into C:\foo\bar
//	\??\UNC\foo\bar into \\foo\bar
//	\??\Volume{abc}\ into C:\
func normaliseLinkPath(path string) (string, error) {
	if len(path) < 4 || path[:4] != `\??\` {
		// unexpected path, return it as is
		return path, nil
	}
	// we have path that start with \??\
	s := path[4:]
	switch {
	case len(s) >= 2 && s[1] == ':': // \??\C:\foo\bar
		return s, nil
	case len(s) >= 4 && s[:4] == `UNC\`: // \??\UNC\foo\bar
		return `\\` + s[4:], nil
	}

	// handle paths, like \??\Volume{abc}\...

	err := windows.LoadGetFinalPathNameByHandle()
	if err != nil {
		// we must be using old version of Windows
		return "", err
	}

	h, err := openSymlink(path)
	if err != nil {
		return "", err
	}
	defer syscall.CloseHandle(h)

	buf := make([]uint16, 100)
	for {
		n, err := windows.GetFinalPathNameByHandle(h, &buf[0], uint32(len(buf)), windows.VOLUME_NAME_DOS)
		if err != nil {
			return "", err
		}
		if n < uint32(len(buf)) {
			break
		}
		buf = make([]uint16, n)
	}
	s = syscall.UTF16ToString(buf)
	if len(s) > 4 && s[:4] == `\\?\` {
		s = s[4:]
		if len(s) > 3 && s[:3] == `UNC` {
			// return path like \\server\share\...
			return `\` + s[3:], nil
		}
		return s, nil
	}
	return "", errors.New("GetFinalPathNameByHandle returned unexpected path: " + s)
}

func readlink(path string) (string, error) {
	h, err := openSymlink(path)
	if err != nil {
		return "", err
	}
	defer syscall.CloseHandle(h)

	rdbbuf := make([]byte, syscall.MAXIMUM_REPARSE_DATA_BUFFER_SIZE)
	var bytesReturned uint32
	err = syscall.DeviceIoControl(h, syscall.FSCTL_GET_REPARSE_POINT, nil, 0, &rdbbuf[0], uint32(len(rdbbuf)), &bytesReturned, nil)
	if err != nil {
		return "", err
	}

	rdb := (*windows.REPARSE_DATA_BUFFER)(unsafe.Pointer(&rdbbuf[0]))
	switch rdb.ReparseTag {
	case syscall.IO_REPARSE_TAG_SYMLINK:
		rb := (*windows.SymbolicLinkReparseBuffer)(unsafe.Pointer(&rdb.DUMMYUNIONNAME))
		s := rb.Path()
		if rb.Flags&windows.SYMLINK_FLAG_RELATIVE != 0 {
			return s, nil
		}
		return normaliseLinkPath(s)
	case windows.IO_REPARSE_TAG_MOUNT_POINT:
		return normaliseLinkPath((*windows.MountPointReparseBuffer)(unsafe.Pointer(&rdb.DUMMYUNIONNAME)).Path())
	default:
		// the path is not a symlink or junction but another type of reparse
		// point
		return "", syscall.ENOENT
	}
}

// Readlink returns the destination of the named symbolic link.
// If there is an error, it will be of type *PathError.
func Readlink(name string) (string, error) {
	s, err := readlink(fixLongPath(name))
	if err != nil {
		return "", &PathError{Op: "readlink", Path: name, Err: err}
	}
	return s, nil
}

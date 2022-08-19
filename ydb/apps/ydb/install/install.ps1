# The MIT License (MIT)
#
# Copyright (c) 2020 YANDEX LLC
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.


function Create-If-Not-Exists {
param (
  [string]$dir
 )

    if (![System.IO.Directory]::Exists($dir)) {
        [void][System.IO.Directory]::CreateDirectory($dir)
    }
}

function Join-Path-Var {
param (
  [string]$base,
  [string]$a
 )
    if ([string]::IsNullOrEmpty($base)) {
        return $a
    }
    if ($base.Split(";") -contains $a) {
        return $base
    }
    if (-not $base.EndsWith(";")) {
        $base += ";"
    }
    return $base + $a
}

function Add-To-Path {
param (
  [string]$dir
 )
    $env:Path = Join-Path-Var $env:Path $dir
    # [Environment]::GetEnvironmentVariable automatically expands things like %USERPROFILE%
    # and we want to preserve them when setting PATH.
    # https://stackoverflow.com/questions/31547104/how-to-get-the-value-of-the-path-environment-variable-without-expanding-tokens
    $userPath = (Get-Item -path "HKCU:\Environment" ).GetValue('Path', '', 'DoNotExpandEnvironmentNames')
    $userPath = Join-Path-Var $userPath $dir
    [Environment]::SetEnvironmentVariable("Path", $userPath, [System.EnvironmentVariableTarget]::User)
}

function Parse-Bool {
param(
  [string]$s
 )
    if (($s -eq "Yes") -or
            ($s -eq "y") -or
            ($s -eq "Y") -or
            ($s -eq "yes")) {
        return $true
    }
    if (($s -eq "No") -or
            ($s -eq "n") -or
            ($s -eq "N") -or
            ($s -eq "no")) {
        return $false
    }
    throw "non parseable bool '${s}'"
}

function Input-Yes-No {
param (
    [string]$welcome
)
    $input = Read-Host $welcome
    while ($true) {
        $input = $input.Trim()
        try {
            if ($input -eq "") {
                return $true
            }
            return Parse-Bool $input
        } catch {
            $input = Read-Host "Please enter 'y' or 'n', not '${input}'"
            continue
        }
    }
}

$goos = "windows"
$goarch = "386"
# [Environment]::Is64BitOperatingSystem is not enough because it may not return anything on windows 7
if (([Environment]::Is64BitOperatingSystem) -or ($env:PROCESSOR_ARCHITECTURE -eq "AMD64")) {
    $goarch = "amd64"
} else {
    throw "Installation failed. 386 machines are not supported yet."
}

$ydbInstallPath = Join-Path $home "ydb"
$ydbStorageUrl = $env:ydbStorageUrl
if ([string]::IsNullOrEmpty($ydbStorageUrl)) {
    $ydbStorageUrl = "https://storage.yandexcloud.net/yandexcloud-ydb"
}
$ydbVersion = $env:ydbVersion
if ([string]::IsNullOrEmpty($ydbVersion)) {
    $downloader = new-object System.Net.WebClient
    $ydbVersion = $downloader.DownloadString("$ydbStorageUrl/release/stable").Trim()
}

Write-Output "Downloading ydb $ydbVersion"

$tempDir = [IO.Path]::GetTempPath()
$tempDir = Join-Path $tempDir "ydbInstall"
Create-If-Not-Exists $tempDir
$tempYdb = Join-Path $tempDir "ydb.exe"
$url = "${ydbStorageUrl}/release/${ydbVersion}/${goos}/${goarch}/ydb.exe"
(new-object System.Net.WebClient).DownloadFile($url, $tempYdb)
& $tempYdb version
if (-not $?) {
    throw "Installation failed. Please try again later or contact technical support."
}

$binPath = Join-Path $ydbInstallPath "bin"
Create-If-Not-Exists $binPath
$ydb = Join-Path $binPath "ydb.exe"
Move-Item -Force $tempYdb $ydb

if (-not ($env:Path.Split(";") -contains $binPath)) {
    $modify = Input-Yes-No "Add ydb installation dir to your PATH? [Y/n]"
    if ($modify) {
        Add-To-Path $binPath
    } else {
        Write-Host "ydb is installed to ${ydb}"
    }
}

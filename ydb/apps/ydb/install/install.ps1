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

$canonicalInstallDir = Join-Path $env:LOCALAPPDATA "Programs\ydb"
$canonicalBinPath = Join-Path $canonicalInstallDir "ydb.exe"
$legacyInstallDir = Join-Path $home "ydb\bin"
$legacyBinaryPath = Join-Path $legacyInstallDir "ydb.exe"
$legacyBackupPath = Join-Path $legacyInstallDir "ydb.exe_old"
$legacyConfigBackupPath = Join-Path $legacyInstallDir "config.json.bak"
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

Create-If-Not-Exists $canonicalInstallDir
Move-Item -Force $tempYdb $canonicalBinPath
Write-Host "ydb is installed to $canonicalBinPath"

if (Test-Path $legacyBinaryPath) {
    Remove-Item -Force $legacyBinaryPath
    Write-Host "Removed legacy binary $legacyBinaryPath"
}
if (Test-Path $legacyBackupPath) {
    Remove-Item -Force $legacyBackupPath
    Write-Host "Removed legacy backup binary $legacyBackupPath"
}
if (Test-Path $legacyConfigBackupPath) {
    Remove-Item -Force $legacyConfigBackupPath
    Write-Host "Removed legacy update backup $legacyConfigBackupPath"
}

function Normalize-PathString {
param (
  [string]$path
 )
    if ([string]::IsNullOrWhiteSpace($path)) {
        return ""
    }
    $expanded = [System.Environment]::ExpandEnvironmentVariables($path)
    try {
        $full = [System.IO.Path]::GetFullPath($expanded)
    } catch {
        $full = $expanded
    }
    $normalized = $full.Trim().TrimEnd('\','/')
    $normalized = $normalized -replace '/', '\'
    return $normalized.ToLowerInvariant()
}

function Update-UserPath {
param (
  [string]$canonicalDir,
  [string]$legacyDir
 )
    $canonicalNorm = Normalize-PathString $canonicalDir
    $legacyNorm = Normalize-PathString $legacyDir
    $raw = (Get-Item -Path "HKCU:\Environment").GetValue('Path', '', 'DoNotExpandEnvironmentNames')
    $parts = @()
    if (-not [string]::IsNullOrEmpty($raw)) {
        $parts = $raw -split ';'
    }
    $newParts = @()
    $replaced = $false
    $hasCanonical = $false
    foreach ($part in $parts) {
        if ([string]::IsNullOrWhiteSpace($part)) {
            continue
        }
        $normalized = Normalize-PathString $part
        if ($normalized -eq $legacyNorm) {
            $newParts += $canonicalDir
            $replaced = $true
            $hasCanonical = $true
            continue
        }
        if ($normalized -eq $canonicalNorm) {
            $hasCanonical = $true
        }
        $newParts += $part
    }
    if ($replaced) {
        $joined = ($newParts -join ';').Trim(';')
        [Environment]::SetEnvironmentVariable("Path", $joined, [System.EnvironmentVariableTarget]::User)
        $env:Path = [Environment]::ExpandEnvironmentVariables($joined)
        Write-Host "Replaced legacy PATH entry with $canonicalDir"
        return
    }
    if ($hasCanonical) {
        return
    }
    $modify = Input-Yes-No "Add $canonicalDir to your PATH? [Y/n]"
    if ($modify) {
        $newParts = @($newParts + $canonicalDir) | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
        $joined = ($newParts -join ';').Trim(';')
        [Environment]::SetEnvironmentVariable("Path", $joined, [System.EnvironmentVariableTarget]::User)
        $env:Path = [Environment]::ExpandEnvironmentVariables($joined)
        Write-Host "Added $canonicalDir to PATH"
    } else {
        Write-Host "You can add $canonicalDir to PATH later via System Settings."
    }
}

Update-UserPath -canonicalDir $canonicalInstallDir -legacyDir $legacyInstallDir

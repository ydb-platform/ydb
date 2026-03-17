pkgs: attrs: with pkgs; with attrs; rec {

  version = "250.12";

  src = fetchFromGitHub {
    owner = "systemd";
    repo = "systemd-stable";
    rev = "v${version}";
    hash = "sha256-w13gB00123alB9sJtPG5Tdcdsi60abafjBxk+lhpib8=";
  };

  patches = [];
  postPatch = "";

  buildInputs = [
    acl
    audit
    kmod
    libcap
    libuuid
    linuxHeaders
    pam

    lz4
    zstd
    xz
  ];

  mesonFlags = [
    "-Daudit=false"
    "-Ddefault-dnssec=no"
    "-Dgcrypt=false"
    "-Dimportd=false"
    "-Dlibcurl=false"
    "-Dlibidn2=false"

    "-Dlz4=true"
    "-Dxz=true"
    "-Dzstd=true"

    "--prefix=/"
    "-Drootprefix=/"
  ];

  # systemd generates serialization code based on Ubuntu SDK it is compiled against.
  # In order to avoid dependencies on the constants declared in the default NixOS SDK,
  # slip Arcadia one via -isystem.

  ubuntu14_sdk = fetchurl {
    name = "Ubuntu-14_x86-64_SDK";
    url = "http://s3.mds.yandex.net/sandbox-469/1966560555/Ubuntu-14-x86_64-SDK-patched-v3.tar.bz2";
    hash = "sha256-K8r1UZZ+hVbGGujmr71CVuxArSoTm7Vr9fzNPQcXIuU";
    downloadToTemp = true;
    recursiveHash = true;
    postFetch = ''
      mkdir -p $out
      cd $out
      tar xf "$downloadedFile"
    '';
  };

  # FIXME thegeorg@ does not work due to missing
  # typedef long long __kernel_time64_t;
  # and
  NIX_CFLAGS_COMPILE = [
    # --sysroot does not override Nixpkgs include paths.
    "-isystem" "${ubuntu14_sdk}/usr/include"
    "-isystem" "${ubuntu14_sdk}/usr/include/x86_64-linux-gnu"
    "-D__kernel_time64_t=long"
    "-D__kernel_old_time_t=long"
  ];
}

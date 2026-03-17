pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.3.10";

  src = fetchurl {
    url = "https://www.apache.org/dist/serf/${pname}-${version}.tar.bz2";
    sha256 = "sha256-voHvCLqiUW7Np2p3rffe97wyJ+61eLmjO0X3tB3AZOY=";
  };

  # Disable kerberos.
  buildPhase = ''
    scons -j$(nproc) PREFIX="$out" OPENSSL="${openssl.dev}" ZLIB="${zlib.dev}" APR="$(echo "${apr.dev}"/bin/*-config)" APU="$(echo "${aprutil.dev}"/bin/*-config)"
  '';
}

pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.1.1t";
  versionWithUnderscores = "${lib.replaceStrings ["."] ["_"] version}";

  src = fetchFromGitHub {
    owner  = "openssl";
    repo   = "openssl";
    rev    = "OpenSSL_${versionWithUnderscores}";
    hash = "sha256-gI2+Vm67j1+xLvzBb+DF0YFTOHW7myotRsXRzluzSLY=";
  };

  patches = [
    ./dso-none.patch
  ];

  buildInputs = [ zlib ];

  configureFlags = [
    # "shared" builds both shared and static libraries
    "shared"
    "--libdir=lib"
    "--openssldir=etc/ssl"
    "enable-ec_nistp_64_gcc_128"
    "no-dynamic-engine"
    "no-tests"
    "zlib"
  ];
}

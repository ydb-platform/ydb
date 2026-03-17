pkgs: attrs: with pkgs.python310.pkgs; with pkgs; with attrs; rec {
  pname = "uwsgi";
  version = "2.0.31";

  src = fetchPypi {
    inherit pname version;
    hash = "sha256-6PizUMzBBv+TplJHuRNvUpwUv5a5NqxbJkxv+dDHYlc=";
  };

  patches = [];

  nativeBuildInputs = [ python3 pkg-config ];

  buildInputs = attrs.buildInputs ++ [
    libyaml openssl pcre zlib python310Packages.greenlet setuptools wheel
  ] ++ lib.optionals stdenv.isLinux [
    libcap.dev
  ];

  NIX_CFLAGS_COMPILE="-I${python310Packages.greenlet}/include/python3.10";

  UWSGI_INCLUDES = lib.concatStringsSep "," ([
    "${zlib.dev}/include"
  ] ++ lib.optionals stdenv.isLinux [
    "${libcap.dev}/include"
  ]);

  yamakerPostBuild = ''
    python3 setup.py bdist_wheel
  '';
}

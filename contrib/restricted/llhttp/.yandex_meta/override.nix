pkgs: attrs: with pkgs; with attrs; rec {
  version = "9.4.3";

  src = fetchFromGitHub {
    owner = "nodejs";
    repo = "llhttp";
    rev = "release/v${version}";
    hash = "sha256-wz87FgdZn0vtdlTWOZL5/Ujhs/uzSwFMHzQ6D9S7dH8=";
  };

  patches = [];

  cmakeFlags = [
    "-DBUILD_STATIC_LIBS=OFF"
  ];
}

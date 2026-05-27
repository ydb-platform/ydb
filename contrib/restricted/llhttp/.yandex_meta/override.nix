pkgs: attrs: with pkgs; with attrs; rec {
  version = "9.4.1";

  src = fetchFromGitHub {
    owner = "nodejs";
    repo = "llhttp";
    rev = "release/v${version}";
    hash = "sha256-eQoOsJ3lIIGSIfC4atkbUqCAYzCzs5kzTihYaI4jqz0=";
  };

  patches = [];

  cmakeFlags = [
    "-DBUILD_STATIC_LIBS=OFF"
  ];
}

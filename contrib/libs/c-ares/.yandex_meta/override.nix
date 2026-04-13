pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.34.6";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-TDhwFF0/sLZT1aag6mTPBZfnh6iOYNO9lVuf+5eJAP0=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

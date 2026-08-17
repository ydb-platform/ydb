pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.34.8";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-iJbD3AAiZtuOc9yr659IWJcQOr6uSCjhX4rn2iF7qXc=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

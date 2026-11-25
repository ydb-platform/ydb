pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.34.9";

  src = fetchFromGitHub {
    owner = "c-ares";
    repo = "c-ares";
    rev= "v${version}";
    hash = "sha256-JaTNynI1WVyZqK0qbF4EjkOaVg4+ivhoLcyAM8dB8wg=";
  };

  patches = [];

  cmakeFlags = [
    "-DCARES_BUILD_TESTS=OFF"
    "-DCARES_SHARED=ON"
    "-DCARES_STATIC=OFF"
  ];
}

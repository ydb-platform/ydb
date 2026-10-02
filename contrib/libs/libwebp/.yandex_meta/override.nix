pkgs: attrs: with pkgs; with attrs; rec {
  version = "1.6.0";

  src = fetchFromGitHub {
    owner = "webmproject";
    repo = "libwebp";
    rev = "v${version}";
    hash = "sha256-7i4fGBTsTjAkBzCjVqXqX4n22j6dLgF/0mz4ajNA45U=";
  };

  patches = [];
}

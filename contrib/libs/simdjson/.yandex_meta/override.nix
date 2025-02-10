pkgs: attrs: with pkgs; rec {
  version = "3.11.6";

  src = fetchFromGitHub {
    owner = "simdjson";
    repo = "simdjson";
    rev = "v${version}";
    hash = "sha256-599gzxJ53nHVo6MkEICzHxaQl0s1vZRjySSRUxC7ZXA=";
  };

  cmakeFlags = attrs.cmakeFlags ++ [
    "-DSIMDJSON_ENABLE_THREADS=OFF"
  ];
}

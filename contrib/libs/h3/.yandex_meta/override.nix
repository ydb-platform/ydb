pkgs: attrs: with pkgs; with attrs; rec {
  version = "3.7.2";

  src = fetchFromGitHub {
    owner = "uber";
    repo = "h3";
    rev = "v${version}";
    sha256 = "0bvsljfxmjvl23v9gxykc4aynjzh5xfy3wg02bxad7cknr1amx9j";
  };

  patches = [];
}

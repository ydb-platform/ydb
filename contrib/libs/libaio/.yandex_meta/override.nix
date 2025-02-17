pkgs: attrs: with pkgs; rec {
  version = "0.3.113";

  src = fetchgit {
    url = "https://pagure.io/libaio.git";
    rev = "libaio-${version}";
    hash = "sha256-8TofYbwsnenv5GuC6FjkUt9rBTULEb5nhknuxr2ckQg=";
  };
}
